import polars as pl
from survey_kit_data.bls.cex import cex
from survey_kit.utilities.dataframe import summary, columns_from_list
from survey_kit.utilities.dataframe_list import DataFrameList

from survey_kit.statistics.multiple_imputation import mi_ses_from_function
from survey_kit.statistics.calculator import StatCalculator
from survey_kit.statistics.statistics import Statistics
from survey_kit.statistics.replicates import Replicates


from survey_kit import logger



d_cex = cex(2023)


df_fml = d_cex["interview_fmli232"]
df_fml = df_fml.select(pl.all().name.to_lowercase())

df_fml = (
    df_fml.with_columns(pl.col("finlwt21").alias("wtrep00"))
    .rename({f"wtrep0{i}":f"wtrep{i}" for i in range(10)})
)

df_weights = df_fml.select(columns_from_list(df_fml,["newid","wtrep*"])).fill_null(0).collect().lazy()

df_salary = DataFrameList(
    [df_fml.select(["newid",pl.col(f"fsalary{i}").alias("fsalary")]).collect().lazy() for i in range(1,6)]
)

df_salary.pipe(summary)



# %%
logger.info("What statistics do I want:")
logger.info("   In this case, the mean of all variables that start with var_")
stats = Statistics(
    stats=["mean"],
    columns="fsalary",
)

# %%
logger.info("Define the 'replicate' object, which tell is what the weight variables are")
replicates = Replicates(weight_stub="wtrep", df=df_weights)


# %%
logger.info("Arguments that are getting passed to StatCalculator at each run")
arguments = dict(
    statistics=stats,
    replicates=replicates
)


# %%
logger.info("Get the multiple imputation standard errofs by calling StatCalculator")
logger.info("   for each implicate and each replicate factor")
logger.info("   If you had 100 bootstrap weights and 5 imputation draws (implicates)")
logger.info("   the StatCalculator calculation would run 5*100=500 times")
mi_results_seq = mi_ses_from_function(
    delegate=StatCalculator,
    df_implicates=df_salary,
    df_noimputes=df_weights,
    index=["newid"],
    arguments=arguments,
    join_on=["Variable"],
    parallel=False,
)

logger.info("\n\nMI Salary Statistics")
mi_results_seq.print()