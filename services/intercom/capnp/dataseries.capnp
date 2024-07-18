# dataseries.capnp
@0xde2ba8f466ec7cbe;

enum DataType @0xbdf7baaaada58dcd {
	numeric @0;
	text @1;
}

struct DataSeriesMetadata @0x821c3fb4ffd24f27 {
	id @0 :Text;
	name @1 :Text;
	description @2 :Text;
	dataType @3 :DataType;
}

struct NumericDataPoint {
	numeric @0 :Float64;
}

struct TextDataPoint {
	text @0 :Text;
}

struct DataPoint(DataPointType) {
	timestamp @0 :Int64;
	value @1 :DataPointType;
}

struct NumericDataSeries @0xfbe7a844aea55332 {
	metadata @0 :DataSeriesMetadata;
	values @1 :List(DataPoint(NumericDataPoint));
}

struct TextDataSeries @0xd89a838a72415d8c {
	metadata @0 :DataSeriesMetadata;
	values @1 :List(DataPoint(TextDataPoint));
}
