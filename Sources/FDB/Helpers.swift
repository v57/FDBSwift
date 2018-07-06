import Dispatch
import CFDB

public typealias Byte = UInt8
public typealias Bytes = [Byte]

func getErrorInfo(for errno: fdb_error_t) -> String {
    return String(cString: fdb_get_error(errno))
}

extension String {
    var bytes: Bytes {
        return Bytes(self.utf8)
    }
}

extension OpaquePointer {
    func asFuture() -> Future {
        return Future(self)
    }

    @discardableResult func waitForFuture() throws -> Future {
        return try self.asFuture().waitAndCheck()
    }
}

extension UnsafePointer where Pointee == Byte {
    func getBytes(length: Int32) -> Bytes {
        let numItems = Int(length) / MemoryLayout<Byte>.stride
        let buffer = self.withMemoryRebound(to: Byte.self, capacity: numItems) {
            UnsafeBufferPointer(start: $0, count: numItems)
        }
        return Array(buffer)
    }
}

public extension DispatchTime {
    public static func seconds(_ seconds: Int) -> DispatchTime {
        return self.init(uptimeNanoseconds: UInt64(seconds) * 1_000_000_000)
    }

    public init(secondsFromNow seconds: Int) {
        self.init(uptimeNanoseconds: DispatchTime.now().rawValue + DispatchTime.seconds(seconds).rawValue)
    }
}