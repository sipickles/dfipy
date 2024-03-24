import pandas as pd
from sqloxide import parse_sql
from datetime import datetime
from pytz import timezone

from dfi.errors import SQLParseError
from dfi.models.filters import FilterField, FilterOperator, FieldType, Only, TimeRange
from dfi.models.filters.geometry import Polygon
from dfi.models.query_document import QueryDocument
from dfi.models.returns.count import Count
from dfi.models.returns.records import Records

from dfi.services.query import Query


class QueryParameters(dict):
    pass


class QueryDocumentBuilder:
    DIALECT = "generic"

    OPERATOR_MAP = {
        "Lt": FilterOperator.LT,
        "Gt": FilterOperator.GT,
        "Gte": FilterOperator.GTE,
        "Lte": FilterOperator.LTE,
        "Eq": FilterOperator.EQ,
        "NotEq": FilterOperator.NEQ,
        "Between": FilterOperator.BETWEEN,
        # "Outside": FilterOperator.OUTSIDE, # non-standard!
        # "like": None,  # unsupported
        # "in": None,  # unsupported
    }

    def __init__(self, sql: str):
        self.parameters = QueryParameters()
        self.tree_list = parse_sql(sql, QueryDocumentBuilder.DIALECT)
        
        for tree in self.tree_list:
            keyword = list(tree.keys())[0]
            match keyword:
                case "Query":
                    self._query(tree[keyword])
                case _:
                    raise SQLParseError(f"Unsupported keyword: {keyword}")

    def build(self) -> QueryDocument:
        return_model = None
        includes = None

        # First pass to find include columns 
        for column in self.parameters["columns"]:
            match column:
                case "count":
                    pass
                case "records":
                    pass
                case _:
                    if not includes:
                        includes = [column]
                    else:
                        includes.append(column)

        # Second pass for return models
        for column in self.parameters["columns"]:
            match column:
                case "count":
                    if "group_by" in self.parameters:
                        return_model = Count(groupby=self.parameters["group_by"])
                    else:
                        return_model = Count()
                case "records":
                    return_model = Records(include=includes)
                case _:
                    pass

        uid_list = self.parameters["uids"] if "uids" in self.parameters else None

        filter_field_list = []
        if "filters" in self.parameters:  
            for filter_dict in self.parameters["filters"]:
                filter_field = FilterField(
                    name=filter_dict["name"], 
                    field_type=filter_dict["type"],
                    value=filter_dict["value"], 
                    operation=filter_dict["operator"],
                    # nullable=filter_dict["nullable"],
                )
                
                filter_field_list.append(filter_field)

        geometry = None
        if "polygon" in self.parameters:
            geometry = Polygon().from_raw_coords(self.parameters["polygon"])

        time_range = None
        if "time" in self.parameters:
            time_range = TimeRange().from_datetimes(self.parameters["time"][0], self.parameters["time"][1])

        return QueryDocument(
            dataset_id=self.parameters["dataset_id"],
            return_model=return_model,
            uids=uid_list,
            filter_fields=filter_field_list,
            geometry=geometry,
            time_range=time_range,
        )
    
    def _query(self, tree: dict):
        keyword_select = tree["body"]["Select"]
        self._select(keyword_select)

    def _select(self, tree: dict):
        self._projection(tree["projection"])
        self._from(tree["from"])
        self._selection(tree["selection"])
        self._group_by(tree["group_by"])

    def _projection(self, projection_list: list):
        columns = []
        for projection in projection_list:
            expression_name = list(projection)[0]

            # Special case for 'select *'
            if expression_name == "Wildcard":
                columns = ["records", "metadataId", "fields"]
                continue

            expression_list = list(projection.values())
            for expression in expression_list:
                columns.append(expression["Identifier"]["value"])

        self.parameters["columns"] = columns

    def _from(self, from_list: list):
        if len(from_list) > 1:
            raise SQLParseError(f"Only single from statement is supported")
        
        first_from = from_list[0]
        table_name_list = first_from["relation"]["Table"]["name"]
        name_value_list = [x["value"] for x in table_name_list]
        dataset_id = ".".join(name_value_list)
        self.parameters["dataset_id"] = dataset_id

    def _selection(self, tree: dict):
        if not tree:
            return
        
        for op_type, op in tree.items():
            match op_type:
                case "BinaryOp":
                    self._selection_binaryOp(op)
                case "Between":
                    self._selection_between(op)
                case "Function": 
                    self._selection_function(op)
                case _:
                    raise SQLParseError(f"Unhandled op: {op_type}")

    def _group_by(self, tree: dict):
        expression_list = tree["Expressions"]
        for expression in expression_list:
            value = expression["Identifier"]["value"]
            if "group_by" in self.parameters:
                raise SQLParseError("Group by is already set")
            else:
                self.parameters["group_by"] = value
        
    def _selection_binaryOp(self, tree: dict):
        operator = tree["op"]

        # Handle nested comparators
        if operator == 'And':
            left_type = list(tree["left"])[0]
            right_type = list(tree["right"])[0]

            match left_type:
                case "BinaryOp":
                    self._selection_binaryOp(tree["left"]["BinaryOp"])
                case "Between":
                    self._selection_between(tree["left"]["Between"])
                case _:
                    raise SQLParseError(f"Unsupported type: {left_type}")
            
            match right_type:
                case "BinaryOp":
                    self._selection_binaryOp(tree["right"]["BinaryOp"])
                case "Between":
                    self._selection_between(tree["right"]["Between"])
                case _:
                    raise SQLParseError(f"Unsupported type: {right_type}")
                
        else:
            name = tree["left"]["Identifier"]["value"]
            value_dict = tree["right"]["Value"]
            if value_dict == "Null":
                value_type = value_dict
            else:
                value_type = list(value_dict)[0]

            match value_type:
                case "SingleQuotedString":
                    value = value_dict[value_type]
                    # Special cases
                    match name:
                        case "id":
                            self.parameters["uids"] = [value]
                            return
                        case "ip":
                            mapped_type = FieldType.IP
                        case _:
                            mapped_type = FieldType.ENUM

                case "Number":
                    value = int(value_dict[value_type][0])
                    mapped_type = FieldType.SIGNED_NUMBER

                case "Null":
                    value = None
                    mapped_type = FieldType.SIGNED_NUMBER # ????

                case _:
                    raise SQLParseError(f"Unsupported Value type: {value_type}")
        
            mapped_operator = QueryDocumentBuilder.OPERATOR_MAP.get(operator)

            filter_dict = {
                "name": name, 
                "value": value,
                "operator": mapped_operator,
                "type": mapped_type,
            }
            
            if "filters" in self.parameters:
                self.parameters["filters"].append(filter_dict)
            else:
                self.parameters["filters"] = [filter_dict]

    def _selection_between(self, tree: dict):
        name = tree["expr"]["Identifier"]["value"]

        match name:
            case "time":
                low_value = tree["low"]["Value"]
                high_value = tree["high"]["Value"]

                low_value_type = list(low_value)[0]
                high_value_type = list(high_value)[0]
                
                match low_value_type:
                    case "SingleQuotedString":
                        low_str = low_value[low_value_type]
                    case _:
                        raise SQLParseError(f"Unsupported time type: {low_value_type}")
                    
                match high_value_type:
                    case "SingleQuotedString":
                        high_str = high_value[high_value_type]
                    case _:
                        raise SQLParseError(f"Unsupported time type: {high_value_type}")
               
                low_datetime = self._parse_datetime(low_str)
                high_datetime = self._parse_datetime(high_str)
                self.parameters["time"] = (low_datetime, high_datetime)
                
            case _:
                low = int(tree["low"]["Value"]["Number"][0])
                high = int(tree["high"]["Value"]["Number"][0])

                filter_dict = {
                    "name": name, 
                    "value": [low, high],
                    "operator": FilterOperator.BETWEEN,
                    "type": FieldType.SIGNED_NUMBER,
                }
                
                if "filters" in self.parameters:
                    self.parameters["filters"].append(filter_dict)
                else:
                    self.parameters["filters"] = [filter_dict]

    @staticmethod
    def _parse_datetime(time_str: str) -> datetime:
        try: 
            # Millisecond date time
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
        except:    
            try:
                # date time
                dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # date
                dt = datetime.strptime(time_str, "%Y-%m-%d")

        # Needed to pass validation
        return dt.replace(tzinfo=timezone("UTC"))
      

    def _selection_function(self, tree: dict):
        function_count = len(tree["name"])
        if function_count > 1:
            raise SQLParseError("Only a single function is supported")
        
        name = tree["name"][0]["value"]
        args = tree["args"][0]

        match name:
            case "ST_MakePolygon":
                self._ST_MakePolygon(args)
            case "ST_GeomFromText":
                return self._ST_GeomFromLine(args)
            case _:
                raise SQLParseError(f"Unsupported function: {name}")
            
    def _ST_MakePolygon(self, args: dict):
        name = list(args.keys())[0]
        expr = args[name]["Expr"]
        expr_name = list(expr.keys())[0]
        
        match expr_name:
            case "Function":
                polygon_list = self._selection_function(expr[expr_name])
                self.parameters["polygon"] = polygon_list
            case _:
                raise SQLParseError(f"Unsupported expression in ST_MakePolygon: {expr_name}")

    def _ST_GeomFromLine(self, args: dict) -> list[tuple[float]]:
        name = list(args.keys())[0]
        value_dict = args[name]["Expr"]["Value"]
        value_type = list(value_dict.keys())[0]
        value = value_dict[value_type]
        geom_type, value_list = value.replace(")", " ").split("(")
        value_list = value_list.split(",")
        
        match geom_type:
            case "LINESTRING":
                return self._line_string(value_list)
            case _:
                raise SQLParseError(f"Unsupported geometry type: {geom_type}")
            
    def _line_string(self, value_pairs: list[str]) -> list[tuple[float]]:
        vertices = []

        for pair in value_pairs:
            x, y = pair.split()
            vertices.append((float(x), float(y)))

        return vertices

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
