# Где в этих процессах CPython

Снимок wchan и py-spy — в {doc}`/reference/wait-traces`. Номера строк `Modules/timemodule.c` здесь не приводятся: файл CPython на машине не открывался целиком.

| Воркер | Кто спит, пока очереди пусты | Что делает CPython в этот момент |
| --- | --- | --- |
| simple-q | tokio, `futures-timer` | встроенный цикл есть, его поток `python-asyncio` в `ep_poll`. Выборку он не делает |
| Dramatiq | два `ConsumerThread` в `time.sleep` | кадр sleep на этих потоках. Рядом `EventLoopThread` в `epoll` и `WorkerThread` на очереди в памяти |
| Celery | родитель в `epoll` на сокете `BRPOP` | один поток интерпретатора. Ребёнок пула блокируется в `pipe_read`, это тоже не GIL-работа |
| arq | тот же поток, что ведёт asyncio | `EpollSelector.select` внутри `Worker.run`. Другого интерпретатора нет |
| taskiq | ребёнок, тот же поток, что ведёт asyncio | `EpollSelector.select` внутри `start_listen`. Родитель спит отдельно и очередь не читает |

`hrtimer_nanosleep` и `ep_poll` — состояния ядра. Поток, который в них стоит, не исполняет байткод. У Dramatiq это видно прямо: py-spy пишет `time.sleep`, wchan пишет `hrtimer_nanosleep`, и за 0.4 с у процесса 0 переключений. Другие потоки того же процесса (цикл asyncio, рабочий поток) могут взять GIL, когда сами выйдут из своего сна.

У simple-q ожидание задачи в интерпретатор не входит. Ускорять Python-обработчик `fast` бессмысленно, пока период heartbeat больше самого обработчика: в логе между `running actor` и остановкой 57 мс, а `pickup_ms` равен 2484.
