import CFDB
import NIO

internal extension EventLoopFuture {
    func checkingRetryableError(for transaction: AsyncTransaction) -> EventLoopFuture {
        return self.flatMapError { error in
            guard let FDBError = error as? FDB.Error else {
                return self.eventLoop.makeFailedFuture(error)
            }

            let onErrorFuture: FDB.Future = fdb_transaction_on_error(transaction.transaction.pointer, FDBError.errno).asFuture()

            let promise: EventLoopPromise<Value> = self.eventLoop.makePromise()

            onErrorFuture.whenVoidReady {
                promise.fail(FDB.Error.transactionRetry(transaction: transaction.transaction))
            }
            onErrorFuture.whenError(promise.fail)

            return promise.futureResult
        }
    }
}

extension FDB.Transaction {
    public func async(_ eventLoop: EventLoop) -> AsyncTransaction {
        return AsyncTransaction(transaction: self, eventLoop: eventLoop)
    }
}

public class AsyncTransaction {
    public let transaction: FDB.Transaction
    public let eventLoop: EventLoop
    init(transaction: FDB.Transaction, eventLoop: EventLoop) {
        self.transaction = transaction
        self.eventLoop = eventLoop
    }
}

public extension AsyncTransaction {
    private func async(_ transaction: FDB.Transaction) -> AsyncTransaction {
        return transaction.async(eventLoop)
    }
    
    /// Commits current transaction
    ///
    /// - returns: EventLoopFuture with future Void value
    func commit() -> EventLoopFuture<Void> {
        let promise: EventLoopPromise<Void> = eventLoop.makePromise()

        let future: FDB.Future = transaction.commit()
        future.whenVoidReady { promise.succeed(()) }
        future.whenError(promise.fail)

        return promise.futureResult.map { _ in () }
    }

    /// Sets bytes to given key in FDB cluster
    ///
    /// - parameters:
    ///   - key: FDB key
    ///   - value: Bytes value
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func set(key: AnyFDBKey, value: Bytes, commit: Bool = false) -> EventLoopFuture<AsyncTransaction> {
        transaction.set(key: key, value: value)

        var future: EventLoopFuture<AsyncTransaction> = eventLoop.makeSucceededFuture(self)

        if commit {
            future = future
                .flatMap { $0.commit() }
                .map { self }
        }

        return future
    }

    /// Returns bytes value for given key (or `nil` if no key)
    ///
    /// - parameters:
    ///   - key: FDB key
    ///   - snapshot: Snapshot read (i.e. whether this read create a conflict range or not)
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future `Bytes?` tuple value
    func get(
        key: AnyFDBKey,
        snapshot: Bool = false,
        commit: Bool = false
    ) -> EventLoopFuture<Bytes?> {
        let promise: EventLoopPromise<Bytes?> = eventLoop.makePromise()

        do {
            let resultFuture = transaction.get(key: key, snapshot: snapshot)
            try resultFuture.whenBytesReady {
                promise.succeed($0)
            }
            resultFuture.whenError(promise.fail)
        } catch {
            promise.fail(error)
        }

        var future = promise.futureResult

        if commit {
            future = future.flatMap { maybeBytes in
                self
                    .commit()
                    .map { maybeBytes }
            }
        }

        return future
    }


    /// Returns bytes value for given key (or `nil` if no key)
    ///
    /// - parameters:
    ///   - key: FDB key
    ///   - snapshot: Snapshot read (i.e. whether this read create a conflict range or not)
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future `(Bytes?, AsyncTransaction)` tuple value
    func get(
        key: AnyFDBKey,
        snapshot: Bool = false,
        commit: Bool = false
    ) -> EventLoopFuture<(Bytes?, AsyncTransaction)> {
        return self
            .get(key: key, snapshot: snapshot, commit: commit)
            .map { ($0, self) }
    }

    /// Returns a range of keys and their respective values in given key range
    ///
    /// - parameters:
    ///   - begin: Begin key
    ///   - end: End key
    ///   - beginEqual: Should begin key also include exact key value
    ///   - beginOffset: Begin key offset
    ///   - endEqual: Should end key also include exact key value
    ///   - endOffset: End key offset
    ///   - limit: Limit returned key-value pairs (only relevant when `mode` is `.exact`)
    ///   - targetBytes: If non-zero, indicates a soft cap on the combined number of bytes of keys and values to return
    ///   - mode: The manner in which rows are returned (see `FDB.StreamingMode` docs)
    ///   - iteration: If `mode` is `.iterator`, this arg represent current read iteration (should start from 1)
    ///   - snapshot: Snapshot read (i.e. whether this read create a conflict range or not)
    ///   - reverse: If `true`, key-value pairs will be returned in reverse lexicographical order
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future `FDB.KeyValuesResult` tuple value
    func get(
        begin: AnyFDBKey,
        end: AnyFDBKey,
        beginEqual: Bool = false,
        beginOffset: Int32 = 1,
        endEqual: Bool = false,
        endOffset: Int32 = 1,
        limit: Int32 = 0,
        targetBytes: Int32 = 0,
        mode: FDB.StreamingMode = .wantAll,
        iteration: Int32 = 1,
        snapshot: Bool = false,
        reverse: Bool = false,
        commit: Bool = false
    ) -> EventLoopFuture<FDB.KeyValuesResult> {
        let promise: EventLoopPromise<FDB.KeyValuesResult> = eventLoop.makePromise()

        do {
            let future: FDB.Future = transaction.get(
                begin: begin,
                end: end,
                beginEqual: beginEqual,
                beginOffset: beginOffset,
                endEqual: endEqual,
                endOffset: endOffset,
                limit: limit,
                targetBytes: targetBytes,
                mode: mode,
                iteration: iteration,
                snapshot: snapshot,
                reverse: reverse
            )
            try future.whenKeyValuesReady {
                promise.succeed($0)
            }
            future.whenError(promise.fail)
        } catch {
            promise.fail(error)
        }

        var future = promise.futureResult

        if commit {
            future = future.flatMap { result in
                self.commit().map { result }
            }
        }

        return future
    }

    /// Returns a range of keys and their respective values in given key range
    ///
    /// - parameters:
    ///   - begin: Begin key
    ///   - end: End key
    ///   - beginEqual: Should begin key also include exact key value
    ///   - beginOffset: Begin key offset
    ///   - endEqual: Should end key also include exact key value
    ///   - endOffset: End key offset
    ///   - limit: Limit returned key-value pairs (only relevant when `mode` is `.exact`)
    ///   - targetBytes: If non-zero, indicates a soft cap on the combined number of bytes of keys and values to return
    ///   - mode: The manner in which rows are returned (see `FDB.StreamingMode` docs)
    ///   - iteration: If `mode` is `.iterator`, this arg represent current read iteration (should start from 1)
    ///   - snapshot: Snapshot read (i.e. whether this read create a conflict range or not)
    ///   - reverse: If `true`, key-value pairs will be returned in reverse lexicographical order
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future `(FDB.KeyValuesResult, AsyncTransaction)` tuple value
    func get(
        begin: AnyFDBKey,
        end: AnyFDBKey,
        beginEqual: Bool = false,
        beginOffset: Int32 = 1,
        endEqual: Bool = false,
        endOffset: Int32 = 1,
        limit: Int32 = 0,
        targetBytes: Int32 = 0,
        mode: FDB.StreamingMode = .wantAll,
        iteration: Int32 = 1,
        snapshot: Bool = false,
        reverse: Bool = false,
        commit: Bool = false
    ) -> EventLoopFuture<(FDB.KeyValuesResult, AsyncTransaction)> {
        return self.get(
            begin: begin,
            end: end,
            beginEqual: beginEqual,
            beginOffset: beginOffset,
            endEqual: endEqual,
            endOffset: endOffset,
            limit: limit,
            targetBytes: targetBytes,
            mode: mode,
            iteration: iteration,
            snapshot: snapshot,
            reverse: reverse,
            commit: commit
        ).map { ($0, self) }
    }

    /// Returns a range of keys and their respective values in given key range
    ///
    /// - parameters:
    ///   - range: Range key
    ///   - beginEqual: Should begin key also include exact key value
    ///   - beginOffset: Begin key offset
    ///   - endEqual: Should end key also include exact key value
    ///   - endOffset: End key offset
    ///   - limit: Limit returned key-value pairs (only relevant when `mode` is `.exact`)
    ///   - targetBytes: If non-zero, indicates a soft cap on the combined number of bytes of keys and values to return
    ///   - mode: The manner in which rows are returned (see `FDB.StreamingMode` docs)
    ///   - iteration: If `mode` is `.iterator`, this arg represent current read iteration (should start from 1)
    ///   - snapshot: Snapshot read (i.e. whether this read create a conflict range or not)
    ///   - reverse: If `true`, key-value pairs will be returned in reverse lexicographical order
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future `(FDB.KeyValuesResult, AsyncTransaction)` tuple value
    func get(
        range: FDB.RangeKey,
        beginEqual: Bool = false,
        beginOffset: Int32 = 1,
        endEqual: Bool = false,
        endOffset: Int32 = 1,
        limit: Int32 = 0,
        targetBytes: Int32 = 0,
        mode: FDB.StreamingMode = .wantAll,
        iteration: Int32 = 1,
        snapshot: Bool = false,
        reverse: Bool = false,
        commit: Bool = false
    ) -> EventLoopFuture<FDB.KeyValuesResult> {
        return self.get(
            begin: range.begin,
            end: range.end,
            beginEqual: beginEqual,
            beginOffset: beginOffset,
            endEqual: endEqual,
            endOffset: endOffset,
            limit: limit,
            targetBytes: targetBytes,
            mode: mode,
            iteration: iteration,
            snapshot: snapshot,
            reverse: reverse,
            commit: commit
        )
    }

    /// Returns a range of keys and their respective values in given key range
    ///
    /// - parameters:
    ///   - range: Range key
    ///   - beginEqual: Should begin key also include exact key value
    ///   - beginOffset: Begin key offset
    ///   - endEqual: Should end key also include exact key value
    ///   - endOffset: End key offset
    ///   - limit: Limit returned key-value pairs (only relevant when `mode` is `.exact`)
    ///   - targetBytes: If non-zero, indicates a soft cap on the combined number of bytes of keys and values to return
    ///   - mode: The manner in which rows are returned (see `FDB.StreamingMode` docs)
    ///   - iteration: If `mode` is `.iterator`, this arg represent current read iteration (should start from 1)
    ///   - snapshot: Snapshot read (i.e. whether this read create a conflict range or not)
    ///   - reverse: If `true`, key-value pairs will be returned in reverse lexicographical order
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future `(FDB.KeyValuesResult, AsyncTransaction)` tuple value
    func get(
        range: FDB.RangeKey,
        beginEqual: Bool = false,
        beginOffset: Int32 = 1,
        endEqual: Bool = false,
        endOffset: Int32 = 1,
        limit: Int32 = 0,
        targetBytes: Int32 = 0,
        mode: FDB.StreamingMode = .wantAll,
        iteration: Int32 = 1,
        snapshot: Bool = false,
        reverse: Bool = false,
        commit: Bool = false
    ) -> EventLoopFuture<(FDB.KeyValuesResult, AsyncTransaction)> {
        return self.get(
            begin: range.begin,
            end: range.end,
            beginEqual: beginEqual,
            beginOffset: beginOffset,
            endEqual: endEqual,
            endOffset: endOffset,
            limit: limit,
            targetBytes: targetBytes,
            mode: mode,
            iteration: iteration,
            snapshot: snapshot,
            reverse: reverse,
            commit: commit
        ).map { ($0, self) }
    }

    /// Performs a generic throwable closure wrapped with event loop sanity check
    ///
    /// - parameters:
    ///   - commit: Whether to commit this transaction after action or not
    ///   - closure: Throwable closure with actual business logic
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    fileprivate func genericAction(
        commit: Bool,
        _ closure: () throws -> Void
    ) -> EventLoopFuture<AsyncTransaction> {
        var future: EventLoopFuture<AsyncTransaction>

        do {
            try closure()
            future = eventLoop.makeSucceededFuture(self)
        } catch {
            return eventLoop.makeFailedFuture(error)
        }

        if commit {
            future = future.flatMap { _ in
                self.commit()
            }.map { self }
        }

        return future
    }

    /// Clears given key in FDB cluster
    ///
    /// - parameters:
    ///   - key: FDB key
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func clear(key: AnyFDBKey, commit: Bool = false) -> EventLoopFuture<AsyncTransaction> {
        return self.genericAction(commit: commit) {
            self.transaction.clear(key: key)
        }
    }

    /// Clears keys in given range in FDB cluster
    ///
    /// - parameters:
    ///   - begin: Begin key
    ///   - end: End key
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func clear(begin: AnyFDBKey, end: AnyFDBKey, commit: Bool = false) -> EventLoopFuture<AsyncTransaction> {
        return self.genericAction(commit: commit) {
            self.transaction.clear(begin: begin, end: end)
        }
    }

    /// Clears keys in given range in FDB cluster
    ///
    /// - parameters:
    ///   - range: Range key
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func clear(range: FDB.RangeKey, commit: Bool = false) -> EventLoopFuture<AsyncTransaction> {
        return self.genericAction(commit: commit) {
            self.transaction.clear(range: range)
        }
    }

    /// Peforms an atomic operation in FDB cluster on given key with given value bytes
    ///
    /// - parameters:
    ///   - _: Atomic operation
    ///   - key: FDB key
    ///   - value: Value bytes
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func atomic(
        _ op: FDB.MutationType,
        key: AnyFDBKey,
        value: Bytes,
        commit: Bool = false
    ) -> EventLoopFuture<AsyncTransaction> {
        return self.genericAction(commit: commit) {
            self.transaction.atomic(op, key: key, value: value)
        }
    }

    /// Peforms an atomic operation in FDB cluster on given key with given generic value
    ///
    /// - parameters:
    ///   - _: Atomic operation
    ///   - key: FDB key
    ///   - value: Value bytes
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func atomic<T>(
        _ op: FDB.MutationType,
        key: AnyFDBKey,
        value: T,
        commit: Bool = false
    ) -> EventLoopFuture<AsyncTransaction> {
        return self.genericAction(commit: commit) {
            self.transaction.atomic(op, key: key, value: getBytes(value))
        }
    }

    /// Sets a transaction option to current transaction
    ///
    /// - parameters:
    ///   - option: Transaction option
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func setOption(_ option: FDB.Transaction.Option) -> EventLoopFuture<AsyncTransaction> {
        return self.genericAction(commit: false) {
            let _: FDB.Transaction = try self.transaction.setOption(option)
        }
    }

    /// Returns transaction snapshot read version
    ///
    /// - returns: EventLoopFuture with future Int64 value
    func getReadVersion() -> EventLoopFuture<Int64> {
        let promise: EventLoopPromise<Int64> = eventLoop.makePromise()

        let future: FDB.Future = self.transaction.getReadVersion()
        future.whenError(promise.fail)

        do {
            try future.whenInt64Ready(promise.succeed)
        } catch {
            promise.fail(error)
        }

        return promise.futureResult
    }
}


public extension FDB {
    /// Executes given transactional closure with appropriate retry logic
    ///
    /// Retry logic kicks in if `notCommitted` (1020) error was thrown during commit event. You must commit
    /// the transaction yourself. Additionally, this transactional closure should be idempotent in order to exclude
    /// unexpected behaviour.
    func withTransaction<T>(
        on eventLoop: EventLoop,
        _ block: @escaping (AsyncTransaction) throws -> EventLoopFuture<T>
        ) -> EventLoopFuture<T> {
        func transactionRoutine(_ transaction: FDB.Transaction) -> EventLoopFuture<T> {
            let result: EventLoopFuture<T>
            
            do {
                let transaction = transaction.async(eventLoop)
                result = try block(transaction).checkingRetryableError(for: transaction)
            } catch {
                result = eventLoop.makeFailedFuture(error)
            }
            
            return result.flatMapError { (error: Swift.Error) -> EventLoopFuture<T> in
                if case let FDB.Error.transactionRetry(transaction) = error {
                    transaction.incrementRetries()
                    return transactionRoutine(transaction)
                }
                return eventLoop.makeFailedFuture(error)
            }
        }
        
        return self
            .begin(on: eventLoop)
            .flatMap(transactionRoutine)
    }
    
}


public extension EventLoopFuture where Value == AsyncTransaction {
    /// Commits current transaction
    ///
    /// - returns: EventLoopFuture with future Void value
    func commit() -> EventLoopFuture<Void> {
        return flatMap { $0.commit() }
    }
    
    /// Sets bytes to given key in FDB cluster
    ///
    /// - parameters:
    ///   - key: FDB key
    ///   - value: Bytes value
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func set(key: AnyFDBKey, value: Bytes, commit: Bool = false) -> EventLoopFuture<AsyncTransaction> {
        return flatMap { $0.set(key: key, value: value, commit: commit) }
    }
    
    /// Returns bytes value for given key (or `nil` if no key)
    ///
    /// - parameters:
    ///   - key: FDB key
    ///   - snapshot: Snapshot read (i.e. whether this read create a conflict range or not)
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future `(Bytes?, FDB.Transaction)` tuple value
    func get(key: AnyFDBKey, snapshot: Bool = false, commit: Bool = false) -> EventLoopFuture<(Bytes?, AsyncTransaction)> {
        return flatMap { $0.get(key: key, snapshot: snapshot, commit: commit) }
    }
    
    /// Returns a range of keys and their respective values in given key range
    ///
    /// - parameters:
    ///   - begin: Begin key
    ///   - end: End key
    ///   - beginEqual: Should begin key also include exact key value
    ///   - beginOffset: Begin key offset
    ///   - endEqual: Should end key also include exact key value
    ///   - endOffset: End key offset
    ///   - limit: Limit returned key-value pairs (only relevant when `mode` is `.exact`)
    ///   - targetBytes: If non-zero, indicates a soft cap on the combined number of bytes of keys and values to return
    ///   - mode: The manner in which rows are returned (see `FDB.StreamingMode` docs)
    ///   - iteration: If `mode` is `.iterator`, this arg represent current read iteration (should start from 1)
    ///   - snapshot: Snapshot read (i.e. whether this read create a conflict range or not)
    ///   - reverse: If `true`, key-value pairs will be returned in reverse lexicographical order
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future `(FDB.KeyValuesResult, FDB.Transaction)` tuple value
    func get(
        begin: AnyFDBKey,
        end: AnyFDBKey,
        beginEqual: Bool = false,
        beginOffset: Int32 = 1,
        endEqual: Bool = false,
        endOffset: Int32 = 1,
        limit: Int32 = 0,
        targetBytes: Int32 = 0,
        mode: FDB.StreamingMode = .wantAll,
        iteration: Int32 = 1,
        snapshot: Bool = false,
        reverse: Bool = false,
        commit: Bool = false
        ) -> EventLoopFuture<(FDB.KeyValuesResult, AsyncTransaction)> {
        return flatMap { $0.get(begin: begin, end: end, beginEqual: beginEqual, beginOffset: beginOffset, endEqual: endEqual, endOffset: endOffset, limit: limit, targetBytes: targetBytes, mode: mode, iteration: iteration, snapshot: snapshot, reverse: reverse, commit: commit) }
    }
    
    /// Returns a range of keys and their respective values in given key range
    ///
    /// - parameters:
    ///   - range: Range key
    ///   - beginEqual: Should begin key also include exact key value
    ///   - beginOffset: Begin key offset
    ///   - endEqual: Should end key also include exact key value
    ///   - endOffset: End key offset
    ///   - limit: Limit returned key-value pairs (only relevant when `mode` is `.exact`)
    ///   - targetBytes: If non-zero, indicates a soft cap on the combined number of bytes of keys and values to return
    ///   - mode: The manner in which rows are returned (see `FDB.StreamingMode` docs)
    ///   - iteration: If `mode` is `.iterator`, this arg represent current read iteration (should start from 1)
    ///   - snapshot: Snapshot read (i.e. whether this read create a conflict range or not)
    ///   - reverse: If `true`, key-value pairs will be returned in reverse lexicographical order
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future `(FDB.KeyValuesResult, FDB.Transaction)` tuple value
    func get(
        range: FDB.RangeKey,
        beginEqual: Bool = false,
        beginOffset: Int32 = 1,
        endEqual: Bool = false,
        endOffset: Int32 = 1,
        limit: Int32 = 0,
        targetBytes: Int32 = 0,
        mode: FDB.StreamingMode = .wantAll,
        iteration: Int32 = 1,
        snapshot: Bool = false,
        reverse: Bool = false,
        commit: Bool = false
        ) -> EventLoopFuture<(FDB.KeyValuesResult, AsyncTransaction)> {
        return flatMap { $0.get(begin: range.begin, end: range.end, beginEqual: beginEqual, beginOffset: beginOffset, endEqual: endEqual, endOffset: endOffset, limit: limit, targetBytes: targetBytes, mode: mode, iteration: iteration, snapshot: snapshot, reverse: reverse, commit: commit) }
    }
    
    /// Clears given key in FDB cluster
    ///
    /// - parameters:
    ///   - key: FDB key
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func clear(key: AnyFDBKey, commit: Bool = false) -> EventLoopFuture<AsyncTransaction> {
        return flatMap { $0.clear(key: key, commit: commit) }
    }
    
    /// Clears keys in given range in FDB cluster
    ///
    /// - parameters:
    ///   - begin: Begin key
    ///   - end: End key
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func clear(begin: AnyFDBKey, end: AnyFDBKey, commit: Bool = false) -> EventLoopFuture<AsyncTransaction> {
        return flatMap { $0.clear(begin: begin, end: end) }
    }
    
    /// Clears keys in given range in FDB cluster
    ///
    /// - parameters:
    ///   - range: Range key
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func clear(range: FDB.RangeKey, commit: Bool = false) -> EventLoopFuture<AsyncTransaction> {
        return flatMap { $0.clear(range: range, commit: commit) }
    }
    
    /// Peforms an atomic operation in FDB cluster on given key with given value bytes
    ///
    /// - parameters:
    ///   - _: Atomic operation
    ///   - key: FDB key
    ///   - value: Value bytes
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func atomic(
        _ op: FDB.MutationType,
        key: AnyFDBKey,
        value: Bytes,
        commit: Bool = false
        ) -> EventLoopFuture<AsyncTransaction> {
        return flatMap { $0.atomic(op, key: key, value: value, commit: commit) }
    }
    
    /// Peforms an atomic operation in FDB cluster on given key with given generic value
    ///
    /// - parameters:
    ///   - _: Atomic operation
    ///   - key: FDB key
    ///   - value: Value bytes
    ///   - commit: Whether to commit this transaction after action or not
    ///
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func atomic<T>(
        _ op: FDB.MutationType,
        key: AnyFDBKey,
        value: T,
        commit: Bool = false
        ) -> EventLoopFuture<AsyncTransaction> {
        return flatMap { $0.atomic(op, key: key, value: value, commit: commit) }
    }
    
    /// Sets a transaction option to current transaction
    ///
    /// - parameters:
    ///   - option: Transaction option
    /// - returns: EventLoopFuture with future Transaction (`self`) value
    func setOption(_ option: FDB.Transaction.Option) -> EventLoopFuture<AsyncTransaction> {
        return flatMap { $0.setOption(option) }
    }
    
    /// Returns transaction snapshot read version
    ///
    /// - returns: EventLoopFuture with future Int64 value
    func getReadVersion() -> EventLoopFuture<Int64> {
        return flatMap { $0.getReadVersion() }
    }
}
