import pandas as pd
from dfi.services.query import Query
from dfi.errors import InvalidQueryDocument

from dfi.services.sql_state.state import SQLQueryDocument
from dfi.services.sql_state.state_map import get_state_map


class QueryDocumentBuilder:
    valid_next_keywords = [
        "select",
        # "insert",
        # "explain",
    ]

    def __init__(self, sql: str):
        self.document = SQLQueryDocument()

        statements = [x.strip() for x in sql.split(";")]

        if len(statements) > 1 and len(statements[1]):
            raise RuntimeError("Only a single sql statement is supported")

        self._tokenise(statements[0])

        valid_next_keywords = QueryDocumentBuilder.valid_next_keywords
        tokens = self.tokens

        while len(tokens):
            tokens, valid_next_keywords = self._parse(tokens, valid_next_keywords)

    def _tokenise(self, statement: str):
        # Don't lowercase every token as some references use mixed case
        self.tokens = [x.strip() for x in statement.split()]
        self._group_quoted_strings()

    def _group_quoted_strings(self):
        tokens = []
        while self.tokens:
            token = self.tokens.pop(0)
            quote_count = token.count("'")
            if quote_count == 1:
                next_token = self.tokens.pop(0)
                tokens.append(f"{token} {next_token}")
            else:
                tokens.append(token)

        self.tokens = tokens

    def _parse(self, tokens: list[str], valid_next_keywords: list[str]) -> tuple:
        token = tokens[0].lower()
        if token in valid_next_keywords:
            state_map = get_state_map()
            state = state_map[token]()
            return state.parse(self.document, tokens[1:])
        else:
            raise RuntimeError(f"Unknown first keyword: {token}")

    def __validate(self):
        return self.document.dataset_id is not None

    def build(self):
        if not self.__validate():
            raise InvalidQueryDocument("SQL Query has not generated a valid query document")

        query_document = {
            "datasetId": self.document.dataset_id,
        }

        if len(self.document["return"]):
            query_document["return"] = self.document["return"]
        if len(self.document["filters"]):
            query_document["filters"] = self.document["filters"]

        return query_document


class Sql:
    def __init__(self, query: Query):
        self.__q = query

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    def __str__(self) -> str:
        return f"""Instance of dfi.{self.__class__.__name__}"""

    def query(self, sql: str) -> pd.DataFrame:
        builder = QueryDocumentBuilder(sql)
        query_document = builder.build()
        return self.__q.raw_request(query_document)
