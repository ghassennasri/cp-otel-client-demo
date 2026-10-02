#!/usr/bin/env python3
"""Create ../cp-otel-client-demo.zip with the sources only.

Excluded: .env (secrets), artifacts/ (run reports and logs), build outputs and caches.
"""
import pathlib
import zipfile

root = pathlib.Path(__file__).resolve().parents[1]
target = root.parent / 'cp-otel-client-demo.zip'
excluded = {'.git', '.github', '.venv', '__pycache__', 'artifacts', 'target'}
temporary = target.with_suffix('.zip.tmp')
try:
    with zipfile.ZipFile(temporary, 'w', zipfile.ZIP_DEFLATED) as archive:
        for file in sorted(root.rglob('*')):
            relative = file.relative_to(root)
            if (not file.is_file() or excluded.intersection(relative.parts) or
                    file.name == '.env' or file.suffix == '.pyc'):
                continue
            if file.is_symlink():
                raise RuntimeError(f'Refusing to follow a source symlink: {relative}')
            archive.write(file, pathlib.Path(root.name) / relative)
    with zipfile.ZipFile(temporary) as archive:
        if archive.testzip() is not None:
            raise RuntimeError('Archive CRC check failed')
        count = len(archive.namelist())
    temporary.replace(target)
    print(f'{target}: {count} files, {target.stat().st_size} bytes')
finally:
    temporary.unlink(missing_ok=True)
