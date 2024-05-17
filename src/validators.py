from base import *
from checks import *

# Validator instances for the BUF types customized for their data and checks

buf_1 = Validator(
    type="BUF 1.0", 
    data={
        "columns": ["symbolId", "symbolTypeId", "symbolValue", "exchangeId", "objectId", "symbolStartDate", "symbolEndDate", "activeFlag", "primaryFlag", "processType"],
        "date_columns": ["symbolStartDate", "symbolEndDate"],
        "date_format": '%m/%d/%Y',
        "date_format_desc": "MM/DD/YY",
        "valid_string": r'[\-/\w@*#.:]+|',
        "valid_filename": r'^([-_A-Za-z0-9]+)_(\([A-Za-z0-9._]+\)).csv$',
        "valid_filename_example": "filedescription_000_(username).csv",
        "extension": ".csv",
        "encoding": "ascii",
        "server_url": "mssql://AV1CON2SQLP.prod.mktint.global/master?trusted_connection=yes"
    },
    preprocess=None,
    file_validity_checks=CheckGroup([
        check_file_extension,
        check_file_encoding,
        check_file_name,
        check_all_columns,
        check_blank_values,
        check_valid_characters,
    ]),
    data_validity_checks=CheckGroup([
        check_validation_symbolID,
        check_validation_symbolTypeID,
        check_validation_exchangeID,
        check_validation_activeFlag,
        check_validation_symbolValue,
        check_validation_symbolstartdate,
        check_validation_symbolenddate,
        check_validation_objectID,
        check_validation_primaryFlag,
        check_validation_processType,
        check_unique_values
    ]),
    logic_validity_checks=CheckGroup([
        check_processtype_ud_symbolid,
        check_processtype_i_symbolid,
        check_processtype_iu_activeflag_enddate,
        check_processtype_iu_endgtstart
    ])
)

# ValidatorApp object which bundles all the distinct validators and provides easy interface during application runtime. 

class ValidatorApp:
    VALIDATORS = {
        "BUF 1.0 - Symbol": buf_1, 
        "BUF 2.0 - Security": None, 
        "BUF 3.0 - Entity": None
    }
    VIEW_RESULTS_COLS = ["level", "name", "code", "description", "result", "error_count", "values", "indices", "comments"]

    def __init__(self):
        self.validator: Optional[Validator] = None
        self._uploaded_file: Optional[UploadedFile] = None
        self.file: Optional[File] = None

    def choose(self, validator: str):
        self.validator = self.VALIDATORS[validator]
    
    def validate(self, file: File):
        if self.validator:
            df = self.validator.validate(file)[self.VIEW_RESULTS_COLS]
            return df

    @property
    def uploaded_file(self) -> Optional[UploadedFile]:
        return self._uploaded_file

    @uploaded_file.setter
    def uploaded_file(self, value: UploadedFile):
        self._uploaded_file = value
        self.file = File.from_streamlit(value) if value else None
    
    def reset(self):
        if self.uploaded_file:
            self.uploaded_file = None
