from dfi.services.sql_state.state import State, SQLQueryDocument


class StateGroupBy(State):
    valid_next_keywords = []

    def parse(self, doc: SQLQueryDocument, tokens: list[str]) -> tuple:
        if tokens[1].lower() != "uniqueid":
            raise RuntimeError("Only uniqueId is valid for group by")

        doc["return"].update(
            {
                "groupBy": {
                    "type": "uniqueId",
                }
            }
        )

        return tokens[2:], StateGroupBy.valid_next_keywords
