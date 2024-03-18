from dfi.services.sql_state.state import State, SQLQueryDocument


class StatePolygon(State):
    """
    Expressed with WKT
    https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
    """

    # these signify the end of a list of vertices
    valid_next_keywords = [
        "and",
    ]

    def parse(self, doc: SQLQueryDocument, tokens: list[str]) -> tuple:
        # Strip end-brackets
        for i in range(2, len(tokens)):
            if "))" in tokens[i]:
                tokens[i] = tokens[i].replace(")", "")
                if not len(tokens[i]):
                    del tokens[i]
                    i -= 1
                break

        if i % 2 == 0:
            raise RuntimeError("Need an even number of values in polygon vertices")

        vertices = []

        for j in range(0, len(tokens), 2):
            if tokens[j].lower() in StatePolygon.valid_next_keywords:
                break

            x = self.cleanup_type(tokens[j])
            y = self.cleanup_type(tokens[j + 1])
            vertices.append([x, y])

        self.prep_filter_dict(doc, "geo")

        doc["filters"]["geo"] = {
            "coordinates": vertices,
            "type": "Polygon",
        }

        return tokens[j:], StatePolygon.valid_next_keywords
