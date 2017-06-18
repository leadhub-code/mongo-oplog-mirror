python3=python3
venv_dir=local/venv

check: $(venv_dir)/packages-installed
	PYTHONDONTWRITEBYTECODE=1 $(venv_dir)/bin/pytest -v tests

$(venv_dir)/packages-installed: setup.py requirements-tests.txt
	test -d $(venv_dir) || $(python3) -m venv $(venv_dir)
	$(venv_dir)/bin/pip install -U pip wheel
	$(venv_dir)/bin/pip install -r requirements-tests.txt
	$(venv_dir)/bin/pip install -e .
	touch $@
