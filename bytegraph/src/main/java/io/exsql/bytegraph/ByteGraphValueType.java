package io.exsql.bytegraph;

public enum ByteGraphValueType {

    Null(0, 15),
    Boolean(1, 0),
    Short(1, 2),
    Int(1, 4),
    Long(1, 8),
    UShort(2, 2),
    UInt(2, 4),
    ULong(2, 8),
    Float(3, 4),
    Double(3, 8),
    Date(4, 3),
    DateTime(4, 8),
    Duration(5, 8),
    Reference(6, 4),
    String(7, 15),
    Blob(8, 15),
    List(9, 15),
    Structure(10, 15),
    Row(11, 15),
    Symbol(12, 8),
    Unknown(-1, 15);

    public final int typeFlag;
    public final int typeLength;

    ByteGraphValueType(final int typeFlag, final int typeLength) {
        this.typeFlag = typeFlag;
        this.typeLength = typeLength;
    }

    public static ByteGraphValueType readValueTypeFromByte(final byte valueTypeByte) {
        final int typeFlag = readTypeFlagFromByte(valueTypeByte);
        final int typeLength = readTypeLengthFromByte(valueTypeByte);

        switch (typeFlag) {
            case 0: return Null;
            case 1:
                switch (typeLength) {
                    case 0:
                    case 1: return Boolean;
                    case 2: return Short;
                    case 4: return Int;
                    case 8: return Long;
                    default: return Unknown;
                }
            case 2:
                switch (typeLength) {
                    case 2: return UShort;
                    case 4: return UInt;
                    case 8: return ULong;
                    default: return Unknown;
                }
            case 3:
                switch (typeLength) {
                    case 4: return Float;
                    case 8: return Double;
                    default: return Unknown;
                }
            case 4:
                switch (typeLength) {
                    case 3: return Date;
                    case 8: return DateTime;
                    default: return Unknown;
                }
            case 5: return typeLength == 8 ? Duration : Unknown;
            case 6: return typeLength == 4 ? Reference : Unknown;
            case 7: return String;
            case 8: return Blob;
            case 9: return List;
            case 10: return Structure;
            case 11: return Row;
            case 12: return Symbol;
            default: return Unknown;
        }
    }

    public static int readTypeFlagFromByte(final byte valueTypeByte) {
        return (valueTypeByte & 0xF0) >> Integer.BYTES;
    }

    public static int readTypeLengthFromByte(final byte valueTypeByte) {
        return valueTypeByte & 0x0F;
    }

    public static ByteGraphValueType fromName(final String name) {
        switch (name) {
            case "null": return Null;
            case "boolean": return Boolean;
            case "short": return Short;
            case "int": return Int;
            case "long": return Long;
            case "ushort": return UShort;
            case "uint": return UInt;
            case "ulong": return ULong;
            case "float": return Float;
            case "double": return Double;
            case "date": return Date;
            case "datetime": return DateTime;
            case "duration": return Duration;
            case "reference": return Reference;
            case "string": return String;
            case "blob": return Blob;
            case "list": return List;
            case "structure": return Structure;
            case "row": return Row;
            case "symbol": return Symbol;
            default: return Unknown;
        }
    }

}
