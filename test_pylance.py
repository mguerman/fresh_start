import pandas as pd
from typing import Any

def escape_newlines_in_df(df: pd.DataFrame) -> pd.DataFrame:
    def _escape(val: Any) -> Any:
        if pd.isna(val) or val == '\\N':
            return val
        if isinstance(val, str):
            return val.replace('\n', '\\n').replace('\r', '\\n')
        return val

    str_cols = df.select_dtypes(include=['object']).columns
    df = df.copy()
    df[str_cols] = df[str_cols].applymap(_escape)
    return df

df = pd.DataFrame({"a": ["hello\nworld", "line\rreturn", None, "\\N"]})
print(escape_newlines_in_df(df))