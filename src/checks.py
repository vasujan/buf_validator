from collections import namedtuple
from base import *
import re

# Functions

def func_file_extension(check: Check, file: File) -> Result:
    file_extension = file.type().extension
    is_valid = file_extension == check.data["extension"]
    return Result(
        result = is_valid, 
        error_count = int(not is_valid),
        values = None if is_valid else [file_extension]
    )

def func_file_encoding(check: Check, file: File) -> Result:
    file_encoding = file.type().encoding
    is_valid = file_encoding == check.data["encoding"]
    return Result(
        result = is_valid, 
        error_count = int(not is_valid),
        values = None if is_valid else [file_encoding],
        comments=None if is_valid else "Make sure to save the file as CSV (Comma delimited) (*.csv) and not CSV UTF-8 (Comma delimited) (*.csv)"
    )

def func_file_name(check: Check, file: File) -> Result:
    file_name_pattern = check.data["valid_filename"]
    match = re.fullmatch(file_name_pattern, file.path.name)
    r = Result.from_bool(bool(match))
    r.values = None if bool(match) else [file.path.name]
    return r

def func_blank_values(check: Check, file: File) -> Result:
    file.df[check.code] = ~(file.base_df.eq('')).any(axis=1)
    r = Result.from_col(file.df[check.code])
    if not r.result:
        r.comments = "Replace blank values with NULL."
    return r

def func_valid_characters(check: Check, file: File) -> Result:
    valid_string = check.data["valid_string"]
    valid_cells = file.base_df.apply(lambda r: r.str.fullmatch(valid_string), axis=1)
    file.df[check.code] = valid_cells.all(axis=1)
    r = Result.from_col(file.df[check.code])

    if r.error_count:
        invalid_values = file.base_df.melt()[(valid_cells == False).melt()['value']]['value']
        invalid_values = invalid_values[~invalid_values.eq('')].unique()
        r.values = list(invalid_values)
    return r

def func_all_columns(check: Check, file: File) -> Result:
    columns = check.data["columns"]
    columns_file = list(file.base_df.columns)
    pair = namedtuple("Column", ['pos', 'expected', 'received'])
    zip_columns = enumerate(zip(columns, columns_file))
    error_pairs = [pair(i+1, c, cf) for i, (c, cf) in zip_columns if c.lower() != cf.lower()]
    r = Result.from_list(error_pairs)
    return r

def func_is_numeric(col: str) -> CheckFunc:
    def check_func(check: Check, file: File) -> Result:
        is_valid = file.df[col].astype(str).str.isnumeric()
        file.df[check.code] = is_valid
        r = Result.from_values(file.df.loc[~file.df[check.code], col])
        return r
    return check_func

def func_is_numeric_or_null(col: str) -> CheckFunc:
    def check_func(check: Check, file: File) -> Result:
        is_null = file.df[col].isna()
        is_valid = file.df[col].astype(str).str.isnumeric()
        file.df[check.code] = is_valid | is_null
        r = Result.from_values(file.df.loc[~file.df[check.code], col])
        return r
    return check_func

def func_is_alphanumeric(col: str) -> CheckFunc:
    def check_func(check: Check, file: File) -> Result:
        is_valid = file.df[col].astype(str).str.isalnum()
        file.df[check.code] = is_valid
        r = Result.from_values(file.df.loc[~is_valid, col])
        return r       
    return check_func

def func_is_proper_date(col: str) -> CheckFunc:
    def check_func(check: Check, file: File) -> Result:
        is_null = file.df[col].isna()
        file.add_dt_cols([col], check.data["date_format"])
        is_valid = file.df[col + '_dt'].notna()
        file.df[check.code] = is_null | is_valid
        r = Result.from_values(file.df.loc[~file.df[check.code], col])
        return r
    return check_func

def func_is_in_values(col: str, values: list[str]) -> CheckFunc:
    def check_func(check: Check, file: File) -> Result:
        file.df[check.code] = file.df[col].astype(str).isin(values)
        r = Result.from_values(file.df.loc[~file.df[check.code], col])
        return r
    return check_func

def func_is_duplicate(cols: list[str]) -> CheckFunc:
    def check_func(check: Check, file: File) -> Result:
        is_dupe = file.df.duplicated(cols, keep=False)
        file.df[check.code] = ~is_dupe
        r = Result.from_col(file.df[check.code])
        return r
    return check_func

def func_processtype_ud_symbolid(check: Check, file: File) -> Result:
    is_process = file.df["processType"].isin(["U", "D"])
    is_valid = file.df["symbolId"].str.isnumeric()
    file.df[check.code] = ~is_process | is_valid
    r = Result.from_col(file.df[check.code])
    return r

def func_processtype_i_symbolid(check: Check, file: File) -> Result:
    is_process = file.df["processType"].isin(["I"])
    is_null = file.df["symbolId"].isna()
    file.df[check.code] = ~is_process | is_null
    r = Result.from_col(file.df[check.code])
    return r

def func_processtype_iu_activeflag_enddate(check: Check, file: File) -> Result:
    is_process = file.df["processType"].isin(["I", "U"])
    is_active = file.df["activeFlag"].isin(["1"])
    is_null = file.df["symbolEndDate"].isna()
    file.df[check.code] = ~is_process | ~is_active | is_null
    r = Result.from_col(file.df[check.code])
    return r

def func_processtype_iu_endgtstart(check: Check, file: File) -> Result:
    is_process = file.df['processType'].isin(['U', 'I'])
    is_na = file.df["symbolStartDate"].isna() | file.df["symbolEndDate"].isna()
    is_nat = file.df["symbolStartDate_dt"].isna() | file.df["symbolEndDate_dt"].isna()
    is_valid = file.df["symbolStartDate_dt"] <= file.df["symbolEndDate_dt"]
    file.df[check.code] = ~is_process | is_valid | is_na | is_nat
    r = Result.from_col(file.df[check.code])
    return r

# Checks

check_file_extension = Check(
    "File", "File extension", "file_extension",
    "File extension must be {extension}.",
    func=func_file_extension
)

check_file_encoding = Check(
    "File", "File encoding", "file_encoding",
    "File encoding must be {encoding}.",
    func=func_file_encoding
)

check_file_name = Check(
    "File", "File name", "file_name",
    "File name must be of type: {valid_filename_example}",
    func=func_file_name
)

check_blank_values = Check(
    "File", "Blank values", "blank_values",
    "File must not have any blank values",
    func=func_blank_values
)

check_valid_characters = Check(
    "File", "Valid characters", "valid_characters",
    "File must have valid characters",
    func=func_valid_characters
)

check_all_columns = Check(
    "File", "Columns", "all_columns",
    "All columns specified must be present in the file in correct order.",
    func=func_all_columns
)

check_validation_symbolID = Check(
    "Data", "SymbolID is numeric", "symbolid_numeric",
    "SymbolID must be numeric or NULL.",
    func=func_is_numeric_or_null("symbolId")
)

check_validation_symbolTypeID = Check(
    "Data", "SymbolTypeID is numeric", "symboltypeid_numeric",
    "SymbolTypeId must be numeric.",
    func=func_is_numeric("symbolTypeId")
)

check_validation_exchangeID = Check(
    "Data", "ExchangeID is numeric", "exchangeid_numeric",
    "ExchangeID must be numeric or NULL.",
    func=func_is_numeric_or_null("exchangeId")
)

check_validation_objectID = Check(
    "Data", "ObjectID is numeric", "objectid_numeric",
    "ObjectID must be numeric.",
    func=func_is_numeric("objectId")
)

check_validation_symbolValue = Check(
    "Data", "SymbolValue is alphanumeric", "symbolvalue_alphanumeric",
    "SymbolValue must be alphanumeric",
    func=func_is_alphanumeric("symbolValue")
)

check_validation_symbolstartdate = Check(
    "Data", "SymbolStartDate is proper date", "symbolstartdate_format",
    "SymbolStartDate can either be NULL or it should have a Date format like {date_format_desc}",
    func=func_is_proper_date("symbolStartDate")
)

check_validation_symbolenddate = Check(
    "Data", "SymbolEndDate is proper date", "symbolenddate_format",
    "SymbolEndDate can either be NULL or it should have a Date format like {date_format_desc}",
    func=func_is_proper_date("symbolEndDate")
)

check_validation_activeFlag = Check(
    "Data", "ActiveFlag Validation", "activeflag_validation",
    "Active Flag can either be 0 or 1.",
    func=func_is_in_values("activeFlag", ['0', '1'])
)

check_validation_primaryFlag = Check(
    "Data", "PrimaryFlag Validation", "primaryflag_validation",
    "Primary Flag can either be 0 or 1.",
    func=func_is_in_values("primaryFlag", ['0', '1'])
)

check_validation_processType = Check(
    "Data", "ProcessType Validation", "processtype_validation",
    "Process Type can either be I, U, D.",
    func=func_is_in_values("processType", ["I", "U", "D"])
)

check_unique_values = Check(
    "Data", "Unique Values", "symbol_dupes",
    "There should not be any dupes for the same symbolvalue, symboltypeId and obectid in the file.",
    func=func_is_duplicate(["symbolValue", "symbolTypeId", "objectId", "symbolId"])
)

check_processtype_ud_symbolid = Check(
    "Logic", "SymbolID populated when U or D ProcessType", "processtype_ud_symbolid",
    "If ProcessType is U or D, symbolId should always be populated",
    func=func_processtype_ud_symbolid
)

check_processtype_i_symbolid = Check(
    "Logic", "SymbolID null when I ProcessType", "processtype_i_symbolid",
    "If ProcessType is I, SymbolId should be NULL",
    func=func_processtype_i_symbolid
)

check_processtype_iu_activeflag_enddate = Check(
    "Logic", "SymbolEndDate null when I, U ProcessType and 1 ActiveFlag", "processtype_iu_activeflag_enddate",
    "If ProcessType is I or U and activeFlag is 1 then SymbolEnddate should be NULL",
    func=func_processtype_iu_activeflag_enddate
)

check_processtype_iu_endgtstart = Check(
    "Logic", "SymbolEndDate is greater than SymbolStartDate", "processtype_iu_endgtstart",
    "If ProcessType is I or U, SymbolStartDate should always be less than SymbolEndDate",
    func=func_processtype_iu_endgtstart
)