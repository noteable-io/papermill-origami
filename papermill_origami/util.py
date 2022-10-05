def removeprefix(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix) :]
    else:
        return s[:]
