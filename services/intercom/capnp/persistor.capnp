# persistor.capnp
@0x9d15012e82f5c7e5;

struct PersistDataSeries {
  id @0 :Text;
  type @1 :DataType;
  values @2 :List(DataPoint);

  enum DataType {
    numerical @0;
    text @1;
    boolean @2;
    arbitrary @3;
  }

  struct DataPoint {
    timestamp @0 :Int64;
    data :union {
      numerical @1 :Float64;
      text @2 :Text;
      boolean @3 :Bool;
      arbitrary @4 :Data;
    }
  }
}

struct FetchDataSeries {
  id @0 :Text;
}