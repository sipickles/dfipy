from dfi.services.sql_state.state import State, SQLQueryDocument


class StateFrom(State):
    valid_states = ["where", "group"]

    def parse(self, doc: SQLQueryDocument, tokens: list[str]) -> tuple:
        doc.dataset_id = self.strip_quotes(tokens[0])

        # Skip the table/dataset
        return tokens[1:], StateFrom.valid_states
