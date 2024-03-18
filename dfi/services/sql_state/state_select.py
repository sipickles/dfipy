from dfi.services.sql_state.state import State, SQLQueryDocument


class StateSelect(State):
    valid_next_keywords = [
        "from",
    ]

    valid_columns = {
        "count": [("return", {"type": "count"})],
        "records": [("return", {"type": "records"})],
        "metadataId": [("return", {"include": ["metadataId"]})],
        "fields": [("return", {"include": ["fields"]})],
        "*": [
            ("return", {"type": "records", "include": ["metadataId", "fields"]})
        ],  # synonym for 'records, metadataId, fields',
        # "newest": [("filters", {"only": "newest"}), ("return", {"type": "records"})],
        # "oldest": [("filters", {"only": "oldest"}), ("return", {"type": "records"})],
    }

    def parse(self, doc: SQLQueryDocument, tokens: list[str]):
        columns = []

        # Look for next keyword
        for i, token in enumerate(tokens):
            token = token.lower()
            if token not in StateSelect.valid_next_keywords:
                columns.append(token.replace(",", "").strip())
            else:
                break

        for column in columns:
            sub_dicts = StateSelect.valid_columns[column]

            for d in sub_dicts:
                sub_dict_name, sub_dict = d
                # include is a special case as its a list which may need extending
                if "include" in sub_dict and "include" in doc[sub_dict_name]:
                    doc[sub_dict_name]["include"].append(sub_dict["include"][0])
                else:
                    doc[sub_dict_name].update(sub_dict)

        return tokens[i:], StateSelect.valid_next_keywords
