# Какие скиллы проверены

Запрос был смотреть [trendshift.io](https://trendshift.io/) и тему [AI skills](https://trendshift.io/topics/ai-skills), и брать только свежие проверяемые скиллы.
Доска дня — продукты (шлюзы, браузеры, модели). Это не каталог `SKILL.md`. Тема AI skills — уже список репозиториев. Звёзды со сторонних витрин не использовались: у нескольких карточек числа расходятся с GitHub API.

Открыты репозитории, дата — `pushed_at` или `created_at` из API 2026-11-13.

| Репозиторий | Что это | Решение |
| --- | --- | --- |
| [diataxis.fr](https://diataxis.fr/) и [dramatiq_prometeus_middware](https://github.com/hewimetall/dramatiq_prometeus_middware) | раскладка tutorial / how-to / reference / explanation | по ней собраны эти страницы |
| [Imbad0202/academic-research-skills](https://github.com/Imbad0202/academic-research-skills) v3.24.2, push 2026-11-11, лицензия CC BY-NC | конвейер статьи: исследование, текст, рецензия, правка. Сам README пишет, что проверка цитат и опытов остаётся за человеком и что конвейер не доказывает, что опыт был выполнен | в текст взято одно правило: число из прогона и причина из кода лежат на разных страницах, ссылка ставится на страницу, которую открывали. Плагин Claude не ставился |
| [globalmpc/backendskills](https://github.com/globalmpc/backendskills), создан 2026-11-05, MIT, 53 звезды по API | три скилла на новые HTTP-ручки, гонки записи и миграции. Свой бенч: 25 критических ошибок из 150 без скиллов, 2 из 150 с ними; 20 из 25 — rate limit логина и rename колонки | не ставился. Этот заход не добавляет ручку и не меняет схему |
| [janestreet/agent_skills](https://github.com/janestreet/agent_skills), push 2026-11-03, MIT | один скилл `socratic-tutor`. В `SKILL.md` прямо сказано не включать урок на разовый вопрос | не включался |
| agent-lsp 0.24.2, [agent-lsp.com](https://www.agent-lsp.com/) | уже стоит в `/usr/local/bin/agent-lsp`, конфиг `.cursor/mcp.json`, pyright в `~/.local/bin` | в этом сеансе инструменты `start_lsp` / `go_to_symbol` в список MCP не попали. Строки цитируются по открытым файлам, не по ответу LSP |

С доски темы отброшены без установки: бинарная эксплуатация (`BountyForge-xyz/pwn-skill`), настройка чужой машины по SSH (`kunchenguid/headless`), наборы «100k skills» и любые скиллы, которые просят выполнить чужой install-скрипт. [NVIDIA/SkillSpector](https://github.com/NVIDIA/SkillSpector) в месячном списке Trendshift указан как сканер скиллов. Репозиторий в этом заходе не клонировался и сканером не гонялся.
