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

struct @0xd8ddc4097cb667c1 StoreRecipe {
    id @0 :Text;
    recipe @1 :Text;

    temporalStrategy @2 :union {
        mirror :group {
            mirroredDataSeriesIds @0 :List(Text);
        }
        fixedInterval :group {
            interval @1 :union {
                simpleSchedule @0 :SimpleSchedule;
                cron @1 :Text;
            }
        }
    }
}

enum TemporalStrategy {
    mirror @0;
    fixedInterval @1;
}
