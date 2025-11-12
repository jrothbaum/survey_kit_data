from survey_kit_data.census.cps_asec import parse_data_dictionary, split_dat
from survey_kit_data.utilities.sas_input_reader import read_sas_fwf

from survey_kit.utilities.dataframe import summary

out_hh = parse_data_dictionary(
    "C:/Users/jonro/Downloads/asec2014R_pubuse.dd.txt",
    output_path="C:/Users/jonro/Downloads/asec2014R_pubuse.dd_parsed_hh.txt",
    from_line="HOUSEHOLD RECORD",
    to_line="FAMILY RECORD",
    skip_values="FILLER")

out_pp = parse_data_dictionary(
    "C:/Users/jonro/Downloads/asec2014R_pubuse.dd.txt",
    output_path="C:/Users/jonro/Downloads/asec2014R_pubuse.dd_parsed_pp.txt",
    from_line="PERSON RECORD",
    to_line="",
    skip_values="FILLER")


out_ff = parse_data_dictionary(
    "C:/Users/jonro/Downloads/asec2014R_pubuse.dd.txt",
    output_path="C:/Users/jonro/Downloads/asec2014R_pubuse.dd_parsed_ff.txt",
    from_line="FAMILY RECORD",
    to_line="PERSON RECORD",
    skip_values="FILLER")




split_dat(
    path="C:/Users/jonro/Downloads/asec2014_pubuse_3x8_rerun_v2/asec2014_pubuse_3x8_rerun_v2.dat",
    path_hhld="C:/Users/jonro/Downloads/asec2014_pubuse_3x8_rerun_v2/asec2014_pubuse_3x8_rerun_v2_hhld.dat",
    path_family="C:/Users/jonro/Downloads/asec2014_pubuse_3x8_rerun_v2/asec2014_pubuse_3x8_rerun_v2_family.dat",
    path_person="C:/Users/jonro/Downloads/asec2014_pubuse_3x8_rerun_v2/asec2014_pubuse_3x8_rerun_v2_person.dat",
)


df_hhld = read_sas_fwf("C:/Users/jonro/Downloads/asec2014_pubuse_3x8_rerun_v2/asec2014_pubuse_3x8_rerun_v2_hhld.dat",
                  sas_script_path="C:/Users/jonro/Downloads/asec2014R_pubuse.dd_parsed_hh.txt")
print(df_hhld.schema)
df_hhld.lazy().sink_parquet("C:/Users/jonro/Downloads/asec2014_pubuse_3x8_rerun_v2/asec2014_pubuse_3x8_rerun_v2_hhld.parquet")

del df_hhld


df_family = read_sas_fwf("C:/Users/jonro/Downloads/asec2014_pubuse_3x8_rerun_v2/asec2014_pubuse_3x8_rerun_v2_family.dat",
                  sas_script_path="C:/Users/jonro/Downloads/asec2014R_pubuse.dd_parsed_ff.txt")
print(df_family.schema)
df_family.lazy().sink_parquet("C:/Users/jonro/Downloads/asec2014_pubuse_3x8_rerun_v2/asec2014_pubuse_3x8_rerun_v2_family.parquet")
del df_family


df_person = read_sas_fwf("C:/Users/jonro/Downloads/asec2014_pubuse_3x8_rerun_v2/asec2014_pubuse_3x8_rerun_v2_person.dat",
                  sas_script_path="C:/Users/jonro/Downloads/asec2014R_pubuse.dd_parsed_pp.txt")
print(df_person.schema)
df_person.lazy().sink_parquet("C:/Users/jonro/Downloads/asec2014_pubuse_3x8_rerun_v2/asec2014_pubuse_3x8_rerun_v2_person.parquet")
del df_person
print("H")