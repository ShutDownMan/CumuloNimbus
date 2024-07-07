# dataseries.capnp
@0xde2ba8f466ec7cbe;

# struct DataSeries @0xe0fe9f49d83296c7 {
#     id @0 :Text;
#     values @2 :List(DataPoint);
# }

# enum DataType {
#     numerical @0;
#     text @1;
#     boolean @2;
#     arbitrary @3;
# }

# struct DataPoint @0xe5c336c382aea664 {
#     timestamp @0 :Int64;
#     data :union {
#         numerical @1 :Float64;
#         text @2 :Text;
#         boolean @3 :Bool;
#         arbitrary @4 :Data;
#     }
# }
