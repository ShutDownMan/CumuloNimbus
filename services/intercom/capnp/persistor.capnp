# persistor.capnp
@0x9d15012e82f5c7e5;

using import "./dataseries.capnp".DataSeries;

enum MergeStrategy {
	replace @0;
	mergeHTP @1;
	mergeLTP @2;
	preserve @3;
}

struct PersistDataSeriesOptions @0xc48cf0dab8226b88 {
	mergeStrategy @0 :MergeStrategy;
	name @1 :Text;
}

struct PersistDataSeries(DataPointType) {
	dataseries @0 :DataSeries(DataPointType);

	options @1 :PersistDataSeriesOptions;
}

struct FetchDataSeries @0xdb6b56ed32274b39 {
	id @0 :Text;
}
