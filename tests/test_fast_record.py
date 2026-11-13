"""job_id из очереди не становится путём файла."""

from bench import core


def test_fast_record_ignores_job_id_in_the_path(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("LAT_PATH", raising=False)
    target = tmp_path / "bench-latency.txt"
    monkeypatch.setattr(core, "_DEFAULT_LAT_PATH", str(target))

    core.fast_record("../../app/payload", 3, 30)

    written = list(tmp_path.rglob("*"))
    assert written == [target]
    n, sent, _now = target.read_text().strip().split(",")
    assert (n, sent) == ("3", "30")


def test_fast_record_uses_operator_lat_path(tmp_path, monkeypatch) -> None:
    path = tmp_path / "lat-simpleq.txt"
    monkeypatch.setenv("LAT_PATH", str(path))

    core.fast_record("job-1", 1, 10)
    core.fast_record("job-1", 2, 11)

    lines = path.read_text().splitlines()
    assert [line.split(",")[:2] for line in lines] == [["1", "10"], ["2", "11"]]
    assert all(len(line.split(",")) == 3 for line in lines)
