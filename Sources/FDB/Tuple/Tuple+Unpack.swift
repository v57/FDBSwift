import Foundation

fileprivate func findTerminator(input: Bytes, pos: Int) -> Int {
    let length = input.count
    var _pos = pos
    while true {
        guard let __pos = input[_pos...].firstIndex(of: 0x00) else {
            return length
        }
        _pos = __pos
        if _pos + 1 == length || input[_pos + 1] != 0xFF {
            return _pos
        }
        _pos += 2
    }
}

extension ArraySlice where Element == Byte {
    internal func replaceEscapes() -> Bytes {
        if self.count == 0 {
            return []
        }
        var result = Bytes()
        var pos = self.startIndex
        let lastIndex = self.endIndex - 1
        while true {
            if pos > lastIndex {
                break
            }
            if self[pos] == 0x00 && pos < lastIndex && self[pos + 1] == 0xFF {
                result.append(0x00)
                pos += 2
                continue
            }
            result.append(self[pos])
            pos += 1
        }
        return result
    }
}

extension FDB.Tuple {
    public init(from bytes: Bytes) throws {
        var result: [FDBTuplePackable] = []
        var pos = 0
        let length = bytes.count
        while pos < length {
            let slice = Bytes(bytes[pos...])
            let res = try FDB.Tuple._unpack(slice)
            pos += res.1
            result.append(res.0)
        }
        self.init(result)
    }
    
    internal static func _unpack(_ input: Bytes, _ pos: Int = 0) throws -> (FDBTuplePackable, Int) {
        func sanityCheck(begin: Int, end: Int) throws {
            guard begin >= input.startIndex else {
                FDB.debug("Invalid begin boundary \(begin) (actual: \(input.startIndex)) while parsing \(input)")
                throw FDB.Error.unpackInvalidBoundaries
            }
            guard end <= input.endIndex else {
                FDB.debug("Invalid end boundary \(end) (actual: \(input.endIndex)) while parsing \(input)")
                throw FDB.Error.unpackInvalidBoundaries
            }
        }

        guard input.count > 0 else {
            throw FDB.Error.unpackEmptyInput
        }

        let code = input[pos]
        if code == NULL {
            return (FDB.Null(), pos + 1)
        } else if code == PREFIX_BYTE_STRING {
            let end = findTerminator(input: input, pos: pos + 1)
            try sanityCheck(begin: pos + 1, end: end)
            return (input[(pos + 1) ..< end].replaceEscapes(), end + 1)
        } else if code == PREFIX_UTF_STRING {
            let _pos = pos + 1
            let end = findTerminator(input: input, pos: _pos)
            try sanityCheck(begin: pos + 1, end: end)
            let bytes = input[(pos + 1) ..< end].replaceEscapes()
            guard let string = String(bytes: bytes, encoding: .utf8) else {
                FDB.debug("Could not convert bytes \(bytes) to string (ascii form: '\(String(bytes: bytes, encoding: .ascii)!)')")
                throw FDB.Error.unpackInvalidString
            }
            return (string, end + 1)
        } else if code >= PREFIX_INT_ZERO_CODE && code < PREFIX_POS_INT_END {
            let n = Int(code) - 20
            let begin = pos + 1
            let end = begin + n
            try sanityCheck(begin: begin, end: end)
            return (
                (Array<Byte>(repeating: 0x00, count: 8 - n) + input[begin ..< end]).reversed().cast() as Int,
                end
            )
        } else if code > PREFIX_NEG_INT_START && code < PREFIX_INT_ZERO_CODE {
            let n = 20 - Int(code)
            let begin = pos + 1
            let end = pos + 1 + n
            guard n < sizeLimits.endIndex else {
                throw FDB.Error.unpackTooLargeInt
            }
            try sanityCheck(begin: begin, end: end)
            return (
                (
                    (
                        Array<Byte>(
                            repeating: 0x00,
                            count: 8 - n
                        )
                            + input[begin ..< end]
                    ).reversed().cast() as Int
                ) - sizeLimits[n],
                end
            )
        } else if code == PREFIX_NESTED_TUPLE {
            var result: [FDBTuplePackable] = []
            var end = pos + 1
            while end < input.count {
                if input[end] == 0x00 {
                    if end + 1 < input.count && input[end + 1] == 0xFF {
                        result.append(FDB.Null())
                        end += 2
                    } else {
                        break
                    }
                } else {
                    let _res = try FDB.Tuple._unpack(input, end)
                    result.append(_res.0)
                    end = _res.1
                }
            }
            return (FDB.Tuple(result), end + 1)
        }

        FDB.debug("Unknown tuple code '\(code)' while parsing \(input)")
        throw FDB.Error.unpackUnknownCode
    }
}
