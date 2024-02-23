"""Class for querying for data with DFI Query V1 API."""

import json
import logging

import pandas as pd
from sseclient import SSEClient
from tqdm import tqdm

from dfi.connect import Connect
from dfi.errors import (
    DFIResponseError,
    EventsMissedError,
    NoEventsRecievedError,
    NoFinishMessageReceivedError,
    UnkownMessageReceivedError,
)
from dfi.models import QueryDocument
from dfi.models.filters import FilterField, Only, TimeRange
from dfi.models.filters.geometry import BBox, Polygon
from dfi.models.returns import Count, GroupBy, IncludeField, Records

_logger = logging.getLogger(__name__)


class Query:
    """Class responsible for requests to the Query V1 DFI API.

    It can be accessed via the a dfi.Client class instance or it must be instantiated
    with a dfi.Connect instance as argument.

    :example:
    Access via `Client`

    ```python
    from dfi import Client

    dfi = Client("<token>", "<url>")

    dfi.query
    ```

    :example:
    Access via `Connect`

    ```python
    from dfi import Connect, Query

    connection = dfi.Connect("<token>", "<base_url>")
    Query(connection)
    ```
    """

    def __init__(self, conn: Connect) -> None:
        """Handle queries to DFI Query V1 API.

        :param conn: a Connect instance.
        """
        self.conn = conn
        self._document: dict | None = None

    def __repr__(self) -> str:
        """Class representation."""
        return f"{self.__class__.__name__}({self.conn!r})"

    def __str__(self) -> str:
        """Class string formatting."""
        return f"{self.__class__.__name__}(conn={self.conn!r})"

    @property
    def document(self) -> dict | None:
        """The Query Document used in the last query for data."""
        return self._document

    def instrumentation(
        self,
        dataset_id: str | None = None,
        identity_id: str | None = None,
        before: str | None = None,
        page_size: int | None = None,
    ) -> list[dict]:  # type: ignore[type-arg]
        """Retrieve a list of queries made to datasets.

        By default this will return a list of the 100 most recent queries to any dataset.
        This request is paginated, using the createdBefore and pageSize parameters to page through.

        :::{admonition} Tenant Admins
        :class: tip
        Tenant admins will be able to see any query ran on any dataset in their tenant.
        - filter by dataset
        - filter by identity
        :::

        :::{admonition} Non-Admins
        :class: tip
        Non-admins will only see their own queries.
        - filter by dataset
        :::

        :param dataset_id: Filter results to only include this dataset.
        :param identity_id: Filter results to only include this identity.
        :param before: [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) string. Only retrieve items created before this given time. Defaults to now.
        :param page_size: Number of items to return in the response. Maximum is 500. Default is 100 if omitted.
        :raises: `DFIResponseError`
        :example:

        ```python
        from dfi import Client

        dfi = Client("<token>", "<url>")

        dfi.query.instrumentation()
        ```
        ```python
        [
            {
                "identityId": "user|0ffb434d-f319-463a-8cf1-6ff939244486",
                "datasetId": "gs.dfi",
                "apiVersion": "5.0.2",
                "responseType": "count",
                "result": "success",
                "idCount": 0,
                "timeRange": None,
                "fieldCount": 0,
                "polygonSize": 0,
                "polygonArea": 0,
                "ingressTime": 1708544552000,
                "issuedElapsed": 8,
                "firstByteElapsed": 154,
                "lastByteElapsed": 154,
            },
            {
                "identityId": "user|0ffb434d-f319-463a-8cf1-6ff939244486",
                "datasetId": "gs.test-dataset",
                "apiVersion": "5.0.2",
                "responseType": "items",
                "result": "success",
                "idCount": 0,
                "timeRange": None,
                "fieldCount": 0,
                "polygonSize": 0,
                "polygonArea": 0,
                "ingressTime": 1708541136914,
                "issuedElapsed": 40,
                "firstByteElapsed": 154,
                "lastByteElapsed": 412,
            },
            {
                "identityId": "user|0ffb434d-f319-463a-8cf1-6ff939244486",
                "datasetId": "gs.test-dataset",
                "apiVersion": "5.0.2",
                "responseType": "itemsWithoutPayload",
                "result": "success",
                "idCount": 0,
                "timeRange": None,
                "fieldCount": 0,
                "polygonSize": 0,
                "polygonArea": 0,
                "ingressTime": 1708541108892,
                "issuedElapsed": 11,
                "firstByteElapsed": 156,
                "lastByteElapsed": 255,
            },
            {
                "identityId": "user|0ffb434d-f319-463a-8cf1-6ff939244486",
                "datasetId": "gs.test-dataset",
                "apiVersion": "5.0.2",
                "responseType": "entities",
                "result": "success",
                "idCount": 1,
                "timeRange": 2678400000,
                "fieldCount": 1,
                "polygonSize": 5,
                "polygonArea": 12391399902,
                "ingressTime": 1708540985751,
                "issuedElapsed": 8,
                "firstByteElapsed": 151,
                "lastByteElapsed": 151,
            },
        ]
        ```
        """
        params = {"identityId": identity_id, "datasetId": dataset_id, "before": before, "pageSize": page_size}
        with self.conn.api_get("v1/query/instrumentation", params=params, stream=False) as response:
            response.raise_for_status()
            return response.json()

    def record_counts(
        self,
        dataset_id: str,
        uids: list[str | int] | None = None,
        geometry: Polygon | BBox | None = None,
        time_range: TimeRange | None = None,
        filter_fields: list[FilterField] | None = None,
    ) -> int:
        """Query for the number of records within the filter bounds.

        :param dataset_id: the dataset to be queried.
        :param uids: specifies which uids to search for.
        :param geometry: specifies the spatial bounds to search within.
        :param time_range: specifies the time bounds to search within.
        :param filter_fields: specifies filters on Filter Fields.
        :returns: The number of records stored in the DFI engine.
        :raises:
            - `DFIResponseError`
            - `TimeRangeUndefinedError`
            - `PolygonUndefinedError`
            - `BBoxUndefinedError`
            - `ValueError`
        :example:
        ```python
        from dfi import Client
        from dfi.models.filters import TimeRange
        from dfi.models.filters.geometry import Polygon

        dfi = Client("<token>", "<url>")

        dataset_id = "<dataset id>"

        uids = ["01234567-89AB-CDEF-1234-0123456789AB"]
        time_range = TimeRange().from_strings(min_time="2022-01-01T08:00:00Z", max_time="2022-02-01T08:00:00Z")

        coordinates = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]
        geometry = Polygon().from_raw_coords(coordinates, geojson=True)

        dfi.query.record_counts(
            dataset_id,
            uids=uids,
            time_range=time_range,
            geometry=geometry,
        )
        ```
        """
        query_doc = QueryDocument(
            dataset_id=dataset_id,
            return_model=Count(),
            uids=uids,
            time_range=time_range,
            geometry=geometry,
            filter_fields=filter_fields,
        )
        self._document = query_doc.build()

        with self.conn.api_post("v1/query", payload=self._document) as response:
            client = SSEClient(response)  # type: ignore[arg-type]
            return self._receive_counts(client)

    def unique_id_counts(
        self,
        dataset_id: str,
        uids: list[str | int] | None = None,
        geometry: Polygon | BBox | None = None,
        time_range: TimeRange | None = None,
        filter_fields: list[FilterField] | None = None,
    ) -> dict[str | int, int]:
        """Query for the number of records for each id within the filter bounds.

        :param dataset_id: the dataset to be queried.
        :param uids: specifies which uids to search for.
        :param geometry: specifies the spatial bounds to search within.
        :param time_range: specifies the time bounds to search within.
        :param filter_fields: specifies filters on Filter Fields.
        :returns: The count of records for each id within the bounds.
        :raises:
            - `DFIResponseError`
            - `TimeRangeUndefinedError`
            - `PolygonUndefinedError`
            - `BBoxUndefinedError`
            - `ValueError`
        :example:
        ```python
        from dfi import Client
        from dfi.models.filters import TimeRange
        from dfi.models.filters.geometry import Polygon

        dfi = Client("<token>", "<url>")

        dataset_id = "<dataset id>"

        uids = ["01234567-89AB-CDEF-1234-0123456789AB"]
        time_range = TimeRange().from_strings(min_time="2022-01-01T08:00:00Z", max_time="2022-02-01T08:00:00Z")

        coordinates = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]
        polygon = Polygon().from_raw_coords(coordinates, geojson=True)

        dfi.query.unique_id_counts(
            dataset_id,
            uids=uids,
            time_range=time_range,
            geometry=geometry,
        )
        ```
        """
        query_doc = QueryDocument(
            dataset_id=dataset_id,
            return_model=Count(groupby=GroupBy("uniqueId")),
            uids=uids,
            time_range=time_range,
            geometry=geometry,
            filter_fields=filter_fields,
        )
        self._document = query_doc.build()

        with self.conn.api_post("v1/query", payload=self._document) as response:
            client = SSEClient(response)  # type: ignore[arg-type]
            return self._receive_unique_id_counts(client)

    def records(
        self,
        dataset_id: str,
        uids: list[str | int] | None = None,
        geometry: Polygon | BBox | None = None,
        time_range: TimeRange | None = None,
        only: Only | str | None = None,
        filter_fields: list[FilterField] | None = None,
        include: list[IncludeField | str] | None = None,
    ) -> pd.DataFrame:
        """Query for the records within the filter bounds.

        :param dataset_id: the dataset to be queried.
        :param uids: specifies which uids to search for.
        :param geometry: specifies the spatial bounds to search within.
        :param time_range: specifies the time bounds to search within.
        :param only: specifies that only the newest or oldest record is retuned.
        :param filter_fields: specifies filters on Filter Fields.
        :param include: specifies the extra fields to include in the returned results.
        :returns: The count of records for each id within the bounds.
        :raises:
            - `DFIResponseError`
            - `TimeRangeUndefinedError`
            - `PolygonUndefinedError`
            - `BBoxUndefinedError`
            - `ValueError`
        :example:
        ```python
        from dfi import Client
        from dfi.models.filters import Only, TimeRange
        from dfi.models.filters.geometry import Polygon

        dfi = Client("<token>", "<url>")

        dataset_id = "<dataset id>"

        uids = ["01234567-89AB-CDEF-1234-0123456789AB"]
        time_range = TimeRange().from_strings(min_time="2022-01-01T08:00:00Z", max_time="2022-02-01T08:00:00Z")

        coordinates = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]
        polygon = Polygon().from_raw_coords(coordinates, geojson=True)

        dfi.query.records(
            dataset_id,
            uids=uids,
            time_range=time_range,
            geometry=geometry,
            only=Only("newest"),
            include=["fields", "metadataId"]
        )
        ```
        """
        query_doc = QueryDocument(
            dataset_id=dataset_id,
            return_model=Records(include=include),
            uids=uids,
            time_range=time_range,
            geometry=geometry,
            only=only,
            filter_fields=filter_fields,
        )
        self._document = query_doc.build()

        with self.conn.api_post("v1/query", payload=self._document) as response:
            client = SSEClient(response)  # type: ignore[arg-type]
            return self._receive_records(client)

    def raw_request(self, document: dict) -> pd.DataFrame | list[str] | int:
        """Provide an escape hatch for those who definitely, absolutely, 100% know what they're doing.

        No validation of the query document is done before saying sending off the request.
        The "hold my beer" of queries.

        :param body: The full request body for POST /v1/query.
        """
        self._document = document

        with self.conn.api_post("v1/query", payload=document) as response:
            client = SSEClient(response)  # type: ignore[arg-type]
            match document:
                case {"return": {"type": "count", "groupBy": {"type": "uniqueId"}}}:
                    return self._receive_unique_id_counts(client)
                case {"return": "count"} | {"return": {"type": "count"}}:
                    return self._receive_counts(client)
                case {"return": {"type": "records"}}:
                    return self._receive_records(client)
                case _:
                    # This will only happen in DFI API adds new return types before dfipy is updated.
                    raise ValueError("Unknown return type.")

    def _receive_counts(self, client: SSEClient) -> int:
        """Collect 'count' results by summing the results received.

        :param client: SSE client for response.
        :raises:
            - `DFIResponseError`
            - `UnkownMessageReceivedError`
            - `NoEventsRecievedError`
            - `NoFinishMessageReceivedError`
            - `EventsMissedError`
        """
        counts = 0

        events_list_is_empty = True
        finish_message = False
        messages_received = 0

        for event in (pbar := tqdm(client.events(), disable=not self.conn.progress_bar, maxinterval=0.5, miniters=1)):
            events_list_is_empty = False

            match event.event:
                case "keepAlive":
                    continue
                case "message":
                    messages_received += 1
                    counts += json.loads(event.data)
                    pbar.set_description(f"Collecting {counts:,} counts")
                    continue
                case "finish":
                    finish_message = True
                    messages_sent = json.loads(event.data).get("messageCount")
                    break
                case "queryError":
                    raise DFIResponseError(event.data)
                case _:
                    raise UnkownMessageReceivedError(event)

        if events_list_is_empty:
            raise NoEventsRecievedError("0 events received from DFI API.")

        if not finish_message:
            raise NoFinishMessageReceivedError(
                "Stream ended before finish message received.  Results may not be complete."
            )

        if messages_received != messages_sent:
            raise EventsMissedError(f"Received {messages_received}/{messages_sent} events from DFI API.")

        return counts

    def _receive_unique_id_counts(self, client: SSEClient) -> dict[str | int, int]:
        """Collect 'uniqueIds groupBy' results.

        :param client: SSE client for response.
        :raises:
            - `DFIResponseError`
            - `UnkownMessageReceivedError`
            - `NoEventsRecievedError`
            - `NoFinishMessageReceivedError`
            - `EventsMissedError`
        """
        unique_id_counts = {}
        events_list_is_empty = True
        finish_message = False
        messages_received = 0

        for event in (pbar := tqdm(client.events(), disable=not self.conn.progress_bar, maxinterval=0.5, miniters=1)):
            events_list_is_empty = False

            match event.event:
                case "keepAlive":
                    continue
                case "message":
                    messages_received += 1
                    unique_id_counts.update(json.loads(event.data))
                    pbar.set_description(f"Collecting {len(unique_id_counts):,} id counts.")
                    continue
                case "finish":
                    finish_message = True
                    messages_sent = json.loads(event.data).get("messageCount")
                    break
                case "queryError":
                    raise DFIResponseError(event.data)
                case _:
                    raise UnkownMessageReceivedError(event)

        if events_list_is_empty:
            raise NoEventsRecievedError("0 events received from DFI API.")

        if not finish_message:
            raise NoFinishMessageReceivedError(
                "Stream ended before finish message received.  Results may not be complete."
            )

        if messages_received != messages_sent:
            raise EventsMissedError(f"Received {messages_received}/{messages_sent} events from DFI API.")

        return unique_id_counts

    def _receive_records(self, client: SSEClient) -> pd.DataFrame:
        """Collect 'records' results into Pandas DataFrame.

        :param client: SSE client for response.
        :raises:
            - `DFIResponseError`
            - `UnkownMessageReceivedError`
            - `NoEventsRecievedError`
            - `NoFinishMessageReceivedError`
            - `EventsMissedError`
        """
        records = []
        events_list_is_empty = True
        finish_message = False
        messages_received = 0

        for event in (pbar := tqdm(client.events(), disable=not self.conn.progress_bar, maxinterval=0.5, miniters=1)):
            events_list_is_empty = False

            match event.event:
                case "keepAlive":
                    continue
                case "message":
                    messages_received += 1
                    records += json.loads(event.data)
                    pbar.set_description(f"Collecting {len(records):,} records.")
                    continue
                case "finish":
                    finish_message = True
                    messages_sent = json.loads(event.data).get("messageCount")
                    break
                case "queryError":
                    raise DFIResponseError(event.data)
                case _:
                    raise UnkownMessageReceivedError(event)

        if events_list_is_empty:
            raise NoEventsRecievedError("0 events received from DFI API.")

        if not finish_message:
            raise NoFinishMessageReceivedError(
                "Stream ended before finish message received.  Results may not be complete."
            )

        if messages_received != messages_sent:
            raise EventsMissedError(f"Received {messages_received}/{messages_sent} events from DFI API.")

        if len(records) > 0:
            return pd.DataFrame(records).assign(time=lambda df: pd.to_datetime(df.time))
        else:
            return pd.DataFrame(columns=["id", "coordinate", "time"])
