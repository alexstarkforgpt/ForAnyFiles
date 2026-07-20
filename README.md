# Памятка: хранение файлов в GitHub

Этот репозиторий предназначен для хранения и переноса файлов между компьютерами. Python-код из него **не нужно запускать**, если задача — лишь скопировать файлы.

Репозиторий: <https://github.com/alexstarkforgpt/ForAnyFiles>

## Что уже настроено на этом ПК

- Git for Windows установлен и обновлён.
- GitHub CLI (`gh`) установлен.
- GitHub CLI авторизован в аккаунте `alexstarkforgpt`.
- Локальная копия репозитория находится в `G:\IT\PROJECTS\Codex\Python\ForAnyFiles`.

Пароли, токены и коды подтверждения в репозиторий не сохраняются.

## Скачать файлы через браузер

1. Открыть страницу репозитория по ссылке выше.
2. Чтобы скачать все файлы сразу: нажать **Code** → **Download ZIP**.
3. Распаковать ZIP-архив в нужную папку.
4. Чтобы скачать один файл: открыть его в GitHub и нажать **Download raw file**.

## Скачать файлы через Git

На другом компьютере, где установлен Git, открыть PowerShell и выполнить:

```powershell
git clone https://github.com/alexstarkforgpt/ForAnyFiles.git
```

Команда создаст папку `ForAnyFiles` со всем содержимым репозитория. Если папка уже была скачана раньше, обновить её можно так:

```powershell
cd .\ForAnyFiles
git pull
```

## Загрузить новые или изменённые файлы

### 1. Скопировать нужные файлы в локальную папку репозитория

Скопировать только те файлы, которые нужно сохранить в GitHub, в:

```text
G:\IT\PROJECTS\Codex\Python\ForAnyFiles
```

### 2. Проверить список изменений

Открыть PowerShell и выполнить:

```powershell
cd G:\IT\PROJECTS\Codex\Python\ForAnyFiles
git status
```

Строки `modified:` и `untracked files:` показывают, что именно будет сохранено. Перед загрузкой убедиться, что в списке нет лишнего.

### 3. Добавить только нужные файлы

Пример для четырёх файлов Cbonds:

```powershell
git add -- `
  local_cbonds_adhoc_journal_dev.py `
  local_cbonds_adhoc_journal_dev.txt `
  local_cbonds_adhoc_journal_dev_learning_annotated.py `
  local_cbonds_adhoc_journal_dev_learning_annotated.txt
```

Не использовать `git add .`, если в папке могут быть лишние файлы.

### 4. Создать коммит и отправить его в GitHub

```powershell
git commit -m "Describe the uploaded files"
git push
```

`commit` создаёт локальную запись об изменениях, а `push` отправляет её в GitHub.

## Если GitHub CLI просит авторизацию

В PowerShell выполнить:

```powershell
& "C:\Program Files\GitHub CLI\gh.exe" auth login --hostname github.com --git-protocol https --web
```

Затем:

1. Ввести `Y`, если CLI спросит про GitHub credentials.
2. Скопировать код, который покажет CLI.
3. Открыть <https://github.com/login/device> или дождаться браузера.
4. Ввести код и подтвердить доступ в аккаунте `alexstarkforgpt`.
5. При необходимости подтвердить вход по электронной почте.
6. Вернуться в PowerShell и дождаться строки `Logged in as alexstarkforgpt`.

Код устройства, пароль и токены никому не передавать и не добавлять в файлы.

## Приватность и доступ для других людей

Сейчас репозиторий `ForAnyFiles` приватный. Его содержимое видите только вы и пользователи, которым вы явно предоставили доступ.

### Пригласить пользователя

1. Открыть страницу репозитория в GitHub.
2. Нажать **Settings**.
3. В левой панели выбрать **Collaborators**.
4. Нажать **Add people**.
5. Ввести GitHub-логин пользователя или его e-mail, выбрать человека и подтвердить приглашение.
6. Пользователь должен принять приглашение из письма или в своём аккаунте GitHub.

Для личного приватного репозитория приглашённый collaborator получает доступ на чтение и запись. Роль «только чтение» в личном репозитории не выдаётся.

### Важное ограничение папок

GitHub выдаёт доступ ко всему репозиторию, а не к отдельной папке. Нельзя разрешить человеку менять только одну подпапку и запретить изменять остальные.

Если разным людям нужны разные наборы файлов, создавайте отдельные приватные репозитории внутри аккаунта `alexstarkforgpt`, например:

- `Project-Client-A` — доступ только клиенту A;
- `Project-Team-B` — доступ только команде B;
- `ForAnyFiles` — личное хранилище.

Доступ выдаётся отдельно для каждого репозитория через **Settings** → **Collaborators** → **Add people**.

## Что уже загружено

- `local_cbonds_adhoc_journal_dev.py`
- `local_cbonds_adhoc_journal_dev.txt`
- `local_cbonds_adhoc_journal_dev_learning_annotated.py`
- `local_cbonds_adhoc_journal_dev_learning_annotated.txt`
