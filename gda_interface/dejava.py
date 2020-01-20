# based on http://stackoverflow.com/a/16470856


def parse(f):
    h = lambda s: " ".join("%.2X" % ord(x) for x in s)  # format as hex
    p = lambda s: sum(
        ord(x) * 256 ** i for i, x in enumerate(reversed(s))
    )  # parse integer
    magic = f.read(2)
    assert magic == "\xAC\xED", h(magic)  # STREAM_MAGIC
    assert p(f.read(2)) == 5  # STREAM_VERSION
    handles = []

    def parse_obj():
        b = f.read(1)
        if not b:
            raise StopIteration  # not necessarily the best thing to throw here.
        if b == "\x70":  # p TC_NULL
            return None
        elif b == "\x71":  # q TC_REFERENCE
            handle = p(f.read(4)) - 0x7E0000  # baseWireHandle
            o = handles[handle]
            return o[1]
        elif b == "\x74":  # t TC_STRING
            string = f.read(p(f.read(2))).decode("utf-8")
            handles.append(("TC_STRING", string))
            return string
        elif b == "\x75":  # u TC_ARRAY
            data = []
            cls = parse_obj()
            size = p(f.read(4))
            handles.append(("TC_ARRAY", data))
            assert cls["_name"] in ("[B", "[I"), (cls["_name"], size, f.read(50))
            for x in range(size):
                data.append(f.read({"[B": 1, "[I": 4}[cls["_name"]]))
            return data
        elif b == "\x7E":  # ~ TC_ENUM
            enum = {}
            enum["_cls"] = parse_obj()
            handles.append(("TC_ENUM", enum))
            enum["_name"] = parse_obj()
            return enum
        elif b == "\x72":  # r TC_CLASSDESC
            cls = {"fields": []}
            full_name = f.read(p(f.read(2)))
            cls["_name"] = full_name.split(".")[-1]  # i don't care about full path
            f.read(8)  # uid
            cls["flags"] = f.read(1)
            handles.append(("TC_CLASSDESC", cls))
            assert cls["flags"] in ("\2", "\3", "\x0C", "\x12"), h(cls["flags"])
            b = f.read(2)
            for i in range(p(b)):
                typ = f.read(1)
                name = f.read(p(f.read(2)))
                fcls = parse_obj() if typ in "L[" else ""
                cls["fields"].append(
                    (name, typ, fcls.split("/")[-1])
                )  # don't care about full path
            b = f.read(1)
            assert b == "\x78", h(b)
            cls["parent"] = parse_obj()
            return cls
        # TC_OBJECT
        assert b == "\x73", (h(b), h(f.read(4)), f.read(50))
        obj = {}
        obj["_cls"] = parse_obj()
        obj["_name"] = obj["_cls"]["_name"]
        handle = len(handles)
        parents = [obj["_cls"]]
        while parents[0]["parent"]:
            parents.insert(0, parents[0]["parent"])
        handles.append(("TC_OBJECT", obj))
        for cls in parents:
            for name, typ, fcls in (
                cls["fields"] if cls["flags"] in ("\2", "\3") else []
            ):
                if typ == "I":  # Integer
                    obj[name] = p(f.read(4))
                elif typ == "S":  # Short
                    obj[name] = p(f.read(2))
                elif typ == "J":  # Long
                    obj[name] = p(f.read(8))
                elif typ == "Z":  # Bool
                    b = f.read(1)
                    assert p(b) in (0, 1)
                    obj[name] = bool(p(b))
                elif typ == "F":  # Float
                    obj[name] = h(f.read(4))
                elif typ == "D":  # Double
                    obj[name] = h(f.read(8))
                elif typ in "BC":  # Byte, Char
                    obj[name] = f.read(1)
                elif typ in "L[":  # Object, Array
                    obj[name] = parse_obj()
                else:  # Unknown
                    assert False, (name, typ, fcls)
            if cls["flags"] in ("\3", "\x0C"):  # SC_WRITE_METHOD, SC_BLOCKDATA
                b = f.read(1)
                if b == "\x77":  # see the readObject / writeObject methods
                    block = f.read(p(f.read(1)))
                    if cls["_name"].endswith("HashMap") or cls["_name"].endswith(
                        "Hashtable"
                    ):
                        # http://javasourcecode.org/html/open-source/jdk/jdk-6u23/java/util/HashMap.java.html
                        # http://javasourcecode.org/html/open-source/jdk/jdk-6u23/java/util/Hashtable.java.html
                        assert len(block) == 8, h(block)
                        size = p(block[4:])
                        obj["data"] = []  # python doesn't allow dicts as keys
                        for i in range(size):
                            k = parse_obj()
                            v = parse_obj()
                            obj["data"].append((k, v))
                        try:
                            obj["data"] = dict(obj["data"])
                        except TypeError:
                            pass  # non hashable keys
                    elif cls["_name"].endswith("HashSet"):
                        # http://javasourcecode.org/html/open-source/jdk/jdk-6u23/java/util/HashSet.java.html
                        assert len(block) == 12, h(block)
                        size = p(block[-4:])
                        obj["data"] = []
                        for i in range(size):
                            obj["data"].append(parse_obj())
                    elif cls["_name"].endswith("ArrayList"):
                        # http://javasourcecode.org/html/open-source/jdk/jdk-6u23/java/util/ArrayList.java.html
                        assert len(block) == 4, h(block)
                        obj["data"] = []
                        for i in range(obj["size"]):
                            obj["data"].append(parse_obj())
                    else:
                        assert False, cls["_name"]
                    b = f.read(1)
                assert b == "\x78", h(b) + " " + repr(f.read(50))  # TC_ENDBLOCKDATA
        handles[handle] = ("py", obj)
        return obj

    objs = []
    while 1:
        try:
            objs.append(parse_obj())
        except StopIteration:
            return objs
