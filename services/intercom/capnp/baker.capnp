# baker.capnp
@0x8ace90cb3ebb8f12;

using import "./dataseries.capnp".DataSeries;

struct ComputeDataSeries @0xd0f63a71ca1c7184 {
    id @0 :Text;
    dependencies @1 :List(DataSeries);
}

struct ComputeAndPersistDataSeries @0xdf9bf64da6e19f86 {
    id @0 :Text;
    startAt @1 :Int64;
    endAt @2 :Int64;

    dependencies @3 :List(Text);
}

struct StoreRecipe @0xd8ddc4097cb667c1 {
    id @0 :Text;
    recipe @1 :Text;

    temporalStrategy :union {
        mirror :group {
            mirroredDataSeriesIds @2 :List(Text);
        }
        fixedInterval :group {
            interval :union {
                iso8601Interval @3 :Text;
                cron @4 :Text;
            }
        }
    }

}

enum TemporalStrategy {
    mirror @0;
    fixedInterval @1;
}
