def get_state_map() -> dict:
    # Avoid circular dependency
    from dfi.services.sql_state.state_from import StateFrom
    from dfi.services.sql_state.state_group_by import StateGroupBy
    from dfi.services.sql_state.state_polygon import StatePolygon
    from dfi.services.sql_state.state_select import StateSelect
    from dfi.services.sql_state.state_where import StateWhere

    return {
        "and": StateWhere,
        "from": StateFrom,
        "group": StateGroupBy,
        "polygon": StatePolygon,
        "select": StateSelect,
        "where": StateWhere,
    }
