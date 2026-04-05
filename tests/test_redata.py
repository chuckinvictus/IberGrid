from datetime import datetime

from ibergrid_ml.clients.redata import REDataClient
from ibergrid_ml.config import ForecastSettings
from ibergrid_ml.time import MADRID


def test_fetch_market_prices_parses_pvpc_and_spot(monkeypatch) -> None:
    payload = {
        "included": [
            {
                "attributes": {
                    "title": "PVPC",
                    "values": [{"datetime": "2026-04-02T00:00:00.000+02:00", "value": 81.88}],
                }
            },
            {
                "attributes": {
                    "title": "Precio mercado spot",
                    "values": [{"datetime": "2026-04-02T00:00:00.000+02:00", "value": 11.06}],
                }
            },
        ]
    }

    client = REDataClient(ForecastSettings())
    monkeypatch.setattr(REDataClient, "_fetch", lambda *_args, **_kwargs: payload)
    frame = client.fetch_market_prices(datetime(2026, 4, 2, tzinfo=MADRID), datetime(2026, 4, 2, 23, tzinfo=MADRID))

    assert frame.shape == (2, 3)
    assert set(frame["metric"].to_list()) == {"pvpc", "spot"}


def test_fetch_demand_aggregates_to_hour(monkeypatch) -> None:
    payload = {
        "included": [
            {
                "attributes": {
                    "title": "Real",
                    "values": [
                        {"datetime": "2026-04-02T00:00:00.000+02:00", "value": 20000},
                        {"datetime": "2026-04-02T00:05:00.000+02:00", "value": 22000},
                    ],
                }
            }
        ]
    }

    client = REDataClient(ForecastSettings())
    monkeypatch.setattr(REDataClient, "_fetch", lambda *_args, **_kwargs: payload)
    frame = client.fetch_demand(datetime(2026, 4, 2, tzinfo=MADRID), datetime(2026, 4, 2, 23, tzinfo=MADRID))

    assert frame.shape == (1, 3)
    assert frame[0, "value_mw"] == 21000
