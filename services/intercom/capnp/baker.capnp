# baker.capnp
@0x8ace90cb3ebb8f12;

# @0xd0f63a71ca1c7184
struct ComputeDataSeries(DataSeries) {
	id @0 :Text;

	struct Dependency {
		dependency @0 :DataSeries;
	}
	dependencies @1 :List(Dependency);
}

struct ComputeAndPersistDataSeries @0xdf9bf64da6e19f86 {
	id @0 :Text;
	startAt @1 :Int64;
	endAt @2 :Int64;

	dependencies @3 :List(Text);
}

struct StoreRecipe @0xd8ddc4097cb667c1 {
	id @0 :Text;
	simplifiedExpression @1 :Text;
	wasmExpression @2 :Text;

	name @3 :Text;
	description @4 :Text;

	temporalStrategy :union {
		mirror :group {
			mirroredDataSeriesIds @5 :List(Text);
		}
		fixedInterval :group {
			interval :union {
				iso8601Interval @6 :Text;
				cron @7 :Text;
			}
		}
	}
}

enum TemporalStrategy {
	mirror @0;
	fixedInterval @1;
}

enum InterpolationStrategy {
	noInterpolation @0;
	locf @1;
	lerp @2;
}

struct FetchRecipe @0xd50f2b456abd2e0f {
	id @0 :Text;
}
