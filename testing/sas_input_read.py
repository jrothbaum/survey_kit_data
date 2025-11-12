from survey_kit_data.utilities.sas_input_reader import read_sas_fwf


df = read_sas_fwf("C:/Users/jonro/Downloads/CPS_ASEC_ASCII_REPWGT_2018/CPS_ASEC_ASCII_REPWGT_2018.dat",
                  sas_script_path="C:/Users/jonro/Downloads/CPS_ASEC_ASCII_REPWGT_2018.SAS.txt")

print(df.describe())