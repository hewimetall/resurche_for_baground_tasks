# Как воркер ждёт задачу

Документация собрана по шаблону [Diátaxis](https://diataxis.fr/).
Тот же каркас, что у [dramatiq-prometheus-fix](https://github.com/hewimetall/dramatiq_prometeus_middware):
Sphinx, MyST, отдельные страницы «что наблюдалось» и «почему код так делает».

Предмет этой папки — пустая очередь. Пять воркеров, Redis 7 на `127.0.0.1:6391`,
один процесс и одна единица конкурентности. Основной Redis сюиты на порту 6379 не использовался.

| Раздел | Вопрос | Зачем открывать |
| --- | --- | --- |
| Tutorial | как снять один трейс | `scripts/wait_trace.py` |
| How-to | как прочитать трейс и как сравнить Dramatiq 1.18 | файлы в `results/traces/` |
| Reference | **что** сделал процесс и что написано в коде | команды Redis, wchan, строки |
| Explanation | **почему** задержка такая | таймер, `LPOP`, `BRPOP`, GIL |

Версии стенда: simple-q 1.0.0rc7, Dramatiq 2.2.1 и отдельно 1.18.0, Celery 5.7.0, arq 0.28.0, taskiq 0.13.0 + taskiq-redis 1.2.4, CPython 3.12.3.

```{toctree}
:maxdepth: 1
:caption: Tutorial

tutorials/first-wait-trace
```

```{toctree}
:maxdepth: 1
:caption: How-to

how-to/read-a-wait-trace
how-to/compare-dramatiq-1.18
```

```{toctree}
:maxdepth: 1
:caption: Reference — что

reference/wait-traces
reference/simpleq
reference/dramatiq
reference/celery
reference/arq
reference/taskiq
reference/dramatiq-1.18
reference/skills
```

```{toctree}
:maxdepth: 1
:caption: Explanation — почему

explanation/simpleq
explanation/dramatiq
explanation/celery
explanation/arq
explanation/taskiq
explanation/dramatiq-1.18
explanation/cpython
explanation/out-of-scope
```

```{toctree}
:maxdepth: 1
:caption: Ранее измеренное

requirements
matrix
bench
rollout
resilience
migration
extensibility
```
