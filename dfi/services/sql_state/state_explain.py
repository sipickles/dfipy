from dfi.services.sql_state.state import State, SQLQueryDocument


class StateExplain(State):
    def parse(self, doc: SQLQueryDocument, tokens: list[str]):
        raise NotImplementedError()
