import io
import types
from typing import Optional, Self, Callable
from dataclasses import dataclass, field, asdict
from functools import partial
from pathlib import Path

import pandas as pd
import chardet
from streamlit.runtime.uploaded_file_manager import UploadedFile

# File processing

@dataclass
class FileType:
    extension: str
    encoding: str

class File:
    FALLBACK_ENCODING = "Windows-1252"

    def __init__(self, path: str, bytes: bytes) -> None:
        self.path = Path(path)
        self.bytes = bytes
        self._df: Optional[pd.Dataframe] = None
        self._base_df: Optional[pd.Dataframe] = None
    
    @classmethod
    def from_streamlit(cls, file: UploadedFile):
        return cls(file.name, file.read())


    @classmethod
    def from_path(cls, path: str):
        with open(path, "rb") as f:
            return cls(path, f.read())
    
    @property
    def df(self) -> pd.DataFrame:
        if self._df is None or self._base_df is None:
            self._df = self.get_csv()
            self._base_df = self._df.copy()
        return self._df

    @df.setter
    def df(self, df: pd.DataFrame) -> None:
        self._df = df

    @property
    def base_df(self) -> pd.DataFrame:
        if self._base_df is None:
            return self.df
        return self._base_df

    @base_df.setter 
    def bsae_df(self, df: pd.DataFrame) -> None:
        self._base_df = df
    
    def read_csv(self, encoding: Optional[str]=None) -> pd.DataFrame:
        if encoding is None:
            encoding = self.FALLBACK_ENCODING
        return pd.read_csv(
            io.BytesIO(self.bytes),
            # self.bytes,
            na_values=["NULL", "Null", "null"],
            keep_default_na=False,
            dtype="str",
            encoding=encoding
        )

    def get_csv(self) -> pd.DataFrame:
        try:
            df = self.read_csv(encoding=None)
        except UnicodeDecodeError as e:
            df = self.read_csv()
        except Exception as e:
            required_type = FileType('.csv', 'ascii')
            if self.type() == required_type:
                raise e
            else:
                print(f"{self.type()} not expected. Ensure that the file is {required_type}.")
                df = pd.DataFrame()
        return df

    def type(self) -> FileType:
        extension = self.path.suffix
        encoding = chardet.detect(self.bytes)["encoding"]
        return FileType(extension, encoding)

    def add_dt_cols(self, cols: list[str], format: str, suffix: str = "_dt") -> None:
        partial_datetime = partial(pd.to_datetime, format=format)
        for col in cols:
            if (col + suffix) in self.df.columns:
                continue
            self.df[col + suffix] = self.df[col].apply(partial_datetime)

    def rename_cols(self, columns: list[str]):
        # if len(columns) > len(self.base_df.columns):
        #     raise ValueError("More columns than expected")
        self.base_df.columns = columns
        col_mapper = {before: after for (before, after) in zip(self.df.columns[:len(columns)], columns)}
        self.df.rename(col_mapper, axis=1, inplace=True)

# Results for validations

@dataclass
class ColumnValidity:
    valid: bool
    missing: set[str]

@dataclass
class Result:
    result: bool
    error_count: int
    values: Optional[list] = field(default=None, kw_only=True)
    indices: Optional[pd.Index] = field(default=None, kw_only=True)
    comments: Optional[str] = field(default=None, kw_only=True)

    @classmethod
    def from_col(cls, col: pd.Series) -> Self:
        r = cls(
            result = col.all(),
            error_count = (~col).sum(),
            indices = None if col.all() else col[~col].index
        )
        return r
    
    @classmethod
    def from_bool(cls, result_: bool) -> Self:
        r = cls(
            result=result_,
            error_count = int(not result_)
        )
        return r
    
    @classmethod
    def from_values(cls, values: pd.Series) -> Self:
        r = cls(
            result = len(values) == 0,
            error_count = len(values),
            values = None if values.empty else list(values.unique()),
            indices = None if values.empty else values.index
        )
        return r
    
    @classmethod
    def from_list(cls, values: list) -> Self:
        r = cls(
            result = not bool(values),
            error_count = len(values),
            values = values if bool(values) else None
        )
        return r

    @classmethod
    def column_na(cls, col_val: ColumnValidity) -> 'Result':
        r = cls(
            result := col_val.valid,
            error_count := len(col_val.missing),
            values = list(col_val.missing) if result else None,
            comments = f'Column {"s" if error_count > 1 else ""} {col_val.missing} not found in the file.'
        )
        return r

# Check objects 

CheckFunc = Callable[["Check", File], Result]

@dataclass
class Check:
    level: str
    name: str
    code: str
    description: str
    func: CheckFunc
    data: dict = field(init=False)

    def __post_init__(self):
        self.func = types.MethodType(self.func, self)

    # @classmethod
    # @property
    # def fields(cls):
    #     # return list(cls.__dataclass_fields__.keys())
    #     return ["type", "level", "order", "name", "description"]

    def __repr__(self):
        return f'Check(type={self.level}, name={self.name})'

    # def col_validate(self, col: str | list[str], file: File) -> ColumnValidity:
    #     if col == np.NaN:
    #         return ColumnValidity(True, set())
    #     col_set = set([col]) if isinstance(col, str) else set(col)
    #     file_col_set = set(file.base_df.columns)
    #     col_valid = col_set.issubset(file_col_set)
    #     col_missing = col_set.difference(file_col_set)
    #     return ColumnValidity(col_valid, col_missing)
    
    def check(self, file: File) -> Result:
        # if self.columns is not np.NaN:
        #     col_validity = self.col_validate(self.columns, file)
        #     if not col_validity.valid:
        #         return Result.column_na(col_validity)
        try:
            return self.func(file)
        except Exception as e:
            raise e
            return Result(
                result=False,
                error_count=1,
                comments=f"Check failed due to {e}"
            )

class CheckGroup:
    def __init__(self, checks: list[Check]) -> None:
        self.checks = checks

    def validate(self, file: File) -> pd.DataFrame:
        checks_series = pd.Series(self.checks, name="checks")
        results_series = pd.Series(checks_series.apply(lambda x: x.check(file)), name="results")
        checks_df = pd.json_normalize(checks_series.apply(asdict).to_list(), max_level=0)
        results_df = pd.json_normalize(results_series.apply(asdict).to_list(), max_level=0)
        results = pd.merge(checks_df, results_df, left_index=True, right_index=True)
        return results

    def __iter__(self):
        for check in self.checks:
            yield check
    
    def add_data(self, data):
        for check in self:
            check.data = data
            check.description = check.description.format_map(data)

# Validator which bundles together the Check for a single BUF type

@dataclass
class Validator:
    type: str
    data: dict = field(default=None)
    preprocess: Optional[Callable] = field(default=None)
    file_validity_checks: CheckGroup = field(default=None)
    data_validity_checks: CheckGroup = field(default=None)
    logic_validity_checks: CheckGroup = field(default=None)

    def __post_init__(self):
        self.file_validity_checks.add_data(self.data)
        self.data_validity_checks.add_data(self.data)
        self.logic_validity_checks.add_data(self.data)

    def validate(self, file: File):
        if self.preprocess:
            self.preprocess(file)
        
        # File Validity
        file_validity = self.file_validity_checks.validate(file)
        valid_df = file_validity
        if not file_validity["result"].all():
            return valid_df.reset_index(drop=True)

        # Data Validity
        file.rename_cols(self.data["columns"])
        data_validity = self.data_validity_checks.validate(file)
        valid_df = pd.concat([valid_df, data_validity])
        if not data_validity["result"].all():
            return valid_df.reset_index(drop=True)
    
        # Logic Validity
        logic_validity = self.logic_validity_checks.validate(file)
        valid_df = pd.concat([valid_df, logic_validity])
        return valid_df.reset_index(drop=True)

            