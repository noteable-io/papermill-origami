import nox
import nox_poetry

LINT_PATHS = ["papermill_origami", "noxfile.py"]

nox.options.reuse_existing_virtualenv = True
nox.options.sessions = ["lint"]


@nox_poetry.session(python="3.9")
def lint(session: nox_poetry.Session):
    session.notify("black_check")
    session.notify("flake8")
    session.notify("isort_check")


@nox_poetry.session(python="3.9")
def flake8(session: nox_poetry.Session):
    session.install("flake8")
    session.run("flake8", *LINT_PATHS, "--count", "--show-source", "--statistics", "--benchmark")


@nox_poetry.session(python="3.9")
def black_check(session: nox_poetry.Session):
    session.install("black")
    session.run("black", "--check", *LINT_PATHS)


@nox_poetry.session(python="3.9")
def isort_check(session: nox_poetry.Session):
    session.install("isort")
    session.run("isort", "--diff", "--check", *LINT_PATHS)


@nox_poetry.session(python="3.9")
def blacken(session: nox_poetry.Session):
    session.install("black")
    session.run("black", *LINT_PATHS)


@nox_poetry.session(python="3.9")
def isort_apply(session: nox_poetry.Session):
    session.install("isort")
    session.run("isort", *LINT_PATHS)
