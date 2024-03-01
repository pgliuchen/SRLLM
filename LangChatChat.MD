Langchain-chatchat UI: alb-p411zjykngc6tn8drp.cn-beijing.alb.aliyuncs.com

# Primary_Insured_Date_of_Birth
Mapping Rule: "Primary_Insured_Date_of_Birth" field could normally map to the standard field "primary_insured_date_of_birth".  
Definition: "Primary_Insured_Date_of_Birth" is a date type field and provided by different insurance companies to indicate the birthday of the insured person. 
Fomat: Currently there are 3 valid formats for "Primary_Insured_Date_of_Birth".

|Type    |Valid Format                                                  |Format Explanation                                                     |Example               |
|--------|--------------------------------------------------------------|------------------------------------------------------------------------|----------------------|
|Date|MM/DD/YYYY|Day-Month-Year with leading zeros.The date in string format with "/" to separate different parts. MM represents month in two digits, DD represents day in two digits, and YYYY represents year in four digits. |12/20/1999|
|Date|YYYY-MM-DD|Month-Day-Year with leading zeros.The date in string format with "-" to separate different parts. MM represents month in two digits, DD represents day in two digits, and YYYY represents year in four digits. |1999-12-20|
|Date|Mon-DD-YYYY|Month abbreviation, Day with leading zeros. The date in string format with "-" to separate different parts. Mon represents short name of month in English, DD represents day in two digits, and YYYY represents year in four digits. |Dec 20, 1999|

# Reinsurance_Claims_Amount
Mapping Rule: "Reinsurance_Claims_Amount" field could normally map to the standard field "reinsurance_claims_amount".  
Definition: "Reinsurance_Claims_Amount" is a claimed amount of money paid by insurance company to the insured person. 
Fomat: Currently there are 1 valid format for "Reinsurance_Claims_Amount".

|Type    |Valid Format                                                  |Format Explanation                                                     |Example               |
|--------|--------------------------------------------------------------|------------------------------------------------------------------------|----------------------|
|float|Float value with at most 2 decimal places.|Float value with at most 2 decimal places. Both positive and negative values are valid.|129888.45|
