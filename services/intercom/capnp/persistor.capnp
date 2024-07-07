# persistor.capnp
@0x9d15012e82f5c7e5;

struct PersistDataSeries @0xb1baefc638ce1dc0  {
  id @0 :Text;
  type @1 :DataType;
  values @2 :List(DataPoint);

  enum DataType {
    numerical @0;
    text @1;
    boolean @2;
    arbitrary @3;
  }

  struct DataPoint @0xdb6b56ed32274b39 {
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

# # persistor.capnp
# @0x9d15012e82f5c7e5;

# using import "./dataseries.capnp".DataSeries;

# # persist data series = data series
# struct PersistDataSeries @0xb1baefc638ce1dc0 {
# 	dataSeries @0 :DataSeries;

# 	# options
# 	mergeStrategy @1 :MergeStrategy;
# 	name @2 :Text;
# }

# enum MergeStrategy {
# 	replace @0;
# 	mergeHTP @1;
# 	mergeLTP @2;
# 	preserve @3;
# }

# struct FetchDataSeries @0xdb6b56ed32274b39 {
# 	id @0 :Text;
# }
