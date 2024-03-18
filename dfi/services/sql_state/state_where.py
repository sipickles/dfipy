from dfi.services.sql_state.state import State, SQLQueryDocument


class StateWhere(State):
    valid_next_keywords = [
        "group",
    ]

    valid_geometry = [
        "polygon",
    ]

    comparison_operator_map = {
        "=": "eq",
        "!=": "neq",
        "<>": "neq",
        "<": "lt",
        ">": "gt",
        ">=": "gte",
        "<=": "lte",
    }

    range_operator_map = {
        "between": "between",
        "outside": "outside",  # non-standard!
        "like": None,  # unsupported
        "in": None,  # unsupported
    }

    def parse(self, doc: SQLQueryDocument, tokens: list[str]) -> tuple:
        # Handle case with no spaces around operator
        for op in StateWhere.comparison_operator_map:
            if op in tokens[0]:
                lhs, rhs = tokens[0].split(op)
                tokens[0] = lhs
                tokens.insert(1, op)
                tokens.insert(2, rhs)
                break

        lhs = tokens[0].lower()
        operator = tokens[1].lower()

        if lhs[0:7] in StateWhere.valid_geometry:
            return self._parse_geometry(doc, tokens)
        elif operator in StateWhere.comparison_operator_map:
            return self._parse_comparison(doc, tokens)
        elif operator in StateWhere.range_operator_map:
            return self._parse_range(doc, tokens)
        else:
            raise RuntimeError(f"Unknown operator: {operator}")

    def _parse_geometry(self, doc: SQLQueryDocument, tokens: list[str]) -> tuple:
        token = tokens[0]
        if "((" in token:
            split_token = token.split("((")
            token = split_token[0].lower()
            tokens[0] = token
            if len(split_token[1]):
                tokens.insert(1, split_token[1])

        return tokens, StateWhere.valid_geometry

    def _parse_comparison(self, doc: SQLQueryDocument, tokens: list[str]) -> tuple:
        lhs, operator, rhs = tokens[0:3]
        mapped_operator = StateWhere.comparison_operator_map.get(operator.lower())
        if not mapped_operator:
            raise RuntimeError(f"Unsupported comparison operator: {operator}")

        if lhs.lower() == "id":
            if mapped_operator != "eq":
                raise RuntimeError(f"operator {operator} is not supported when used with id")
            self._add_id_field(doc, rhs)
        else:
            self._add_comparison_field(doc, lhs, mapped_operator, rhs)

        return tokens[4:], StateWhere.valid_next_keywords

    def _parse_range(self, doc: SQLQueryDocument, tokens: list[str]):
        lhs, operator, range_min, _, range_max = tokens[0:5]
        mapped_operator = StateWhere.range_operator_map.get(operator.lower())
        if not mapped_operator:
            raise RuntimeError(f"Unsupported range operator: {operator}")

        if lhs == "time":
            self._add_time_field(doc, mapped_operator, range_min, range_max)
        else:
            self._add_range_field(doc, lhs, mapped_operator, range_min, range_max)

        return tokens[5:], StateWhere.valid_next_keywords

    def _add_id_field(self, doc: SQLQueryDocument, rhs: str):
        self.prep_filter_dict(doc, "id")

        rhs = self.cleanup_type(rhs)
        doc["filters"]["id"] = [rhs]

    def _add_comparison_field(self, doc: SQLQueryDocument, lhs: str, operator: str, rhs: any):
        self.prep_filter_dict(doc, "fields")

        rhs = self.cleanup_type(rhs)
        doc["filters"]["fields"][lhs] = {operator: rhs}

    def _add_range_field(self, doc: SQLQueryDocument, lhs: str, operator: str, range_min: any, range_max: any):
        self.prep_filter_dict(doc, "fields")

        range_min = self.cleanup_type(range_min)
        range_max = self.cleanup_type(range_max)
        doc["filters"]["fields"][lhs] = {operator: [range_min, range_max]}

    def _add_time_field(self, doc: SQLQueryDocument, operator: str, range_min: any, range_max: any):
        if operator != "between":
            raise RuntimeError("Only 'between' is supported with time filters")

        self.prep_filter_dict(doc, "time")

        range_min = self.cleanup_type(range_min)
        range_max = self.cleanup_type(range_max)
        doc["filters"]["time"] = {"minTime": range_min, "maxTime": range_max}
