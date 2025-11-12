import polars as pl
from survey_kit_data.census.cps_asec import cps_asec
from survey_kit.utilities.compress import compress_df
from survey_kit.utilities.dataframe import (
    summary,
    columns_from_list
)
from survey_kit.statistics.calculator import StatCalculator
from survey_kit.statistics.replicates import Replicates
from survey_kit.statistics.statistics import Statistics
from survey_kit import logger, config


n_replicates = 10


logger.info("Load the data")
for yeari in range(2005,2006):
    d_cps = cps_asec(yeari)
    logger.info(f"\n\n{yeari}")
    summary(d_cps["hhld"])


for keyi, dfi in d_cps.items():
    d_cps[keyi] = dfi.select(pl.all().name.to_lowercase())

if "replicate_weights" in d_cps:
    df_rep_weights = d_cps["replicate_weights"]
else:
    df_rep_weights = None
df_hhld = d_cps["hhld"]
df_person = d_cps["person"]


logger.info("Get the columns we need")
df_hhld = df_hhld.select(
    columns_from_list(df_hhld,columns=["*seq","htotval","hrhtype"])
)

df_person = df_person.select(
    columns_from_list(df_person,columns=["*seq","pppos","hhdrel","prdtrace"])
)

if n_replicates > 0 and df_rep_weights is not None:
    df_rep_weights = (
        df_rep_weights.select(
            ["h_seq","pppos"] + 
            [f"pwwgt{i}" for i in range(0,n_replicates + 1)]
        )
    )


logger.info("Merge the data sets together")
df_joined = df_hhld.join(
    df_person,
    how="inner",
    left_on=["h_seq"],
    right_on=["ph_seq"]
)

if df_rep_weights is not None:
    df_joined = compress_df(
        df_joined.join(
            df_rep_weights,
            on=["h_seq","pppos"],
            how="inner"
        ),
        check_string=True
    )

#   Run through the join to avoid repeat load/join at replicate calculation
df_joined = df_joined.collect().lazy()


summary(df_joined)



logger.info("Get the estimates")
if df_rep_weights is not None:
    replicates = Replicates(
        weight_stub="pwwgt",
        df=df_joined
    )
else:
    replicates = None


c_all_households = pl.col("hhdrel").eq(1) & pl.col("hrhtype").lt(9)
c_white_only = pl.col("prdtrace") == 1
sc = StatCalculator(
    df_joined.filter(c_all_households).collect().lazy(),
    # df_joined.filter(c_all_households).collect().to_pandas(),
    statistics=Statistics(
        stats=["n","mean","p25","p50","p75"],
        columns="htotval",
        # quantile_interpolated=True
    ),
    replicates=replicates
)

sc_white = StatCalculator(
    df_joined.filter(c_all_households & c_white_only).collect().lazy(),
    # df_joined.filter(c_all_households & c_white_only).collect().to_pandas(),
    statistics=Statistics(
        stats=["n","mean","p25","p50","p75"],
        columns="htotval",
        # quantile_interpolated=True
    ),
    replicates=replicates
)

sc_all_white = sc.compare(sc_white,display=False)["ratio"]

sc_all_white.print()