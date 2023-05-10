import ocflindex
from stream_unzip import stream_unzip




class Client(ocflindex.Client):
    def pqnews_getday(self, name: str, year: int, month:int, day:int):
        def resolveID( name: str, year: int, month:int, day:int) -> str:
            pubs = {
                "wapost": [('18771206','60400'),('19230101','47009'),('19540318','47011'),('19590827','47012')],
                "nytimes": [('18510918','11558'),('18570914','55428'),('19230101','45545')],
                "latimes": [('18811204','46998'),('18861023','55410'),('19230101','47001')]
            }
            date = f"{year:04}{month:02}{day:02}"
            pubid = ''
            for dateid in pubs[name]:
                if date > dateid[0]:
                    pubid = dateid[1]
                    continue
                if date < dateid[0]:
                    break
                if date == dateid[0]:
                    return dateid[1]        
            return f"{pubid}/{year:04}/{month:02}/{day:02}"
        id = resolveID(name, year, month, day)
        state = self.get_object_state(id, "xml.zip")
        for _, _, unzipped_chunks in stream_unzip(self.content_stream(state.digest)):
            cont = bytearray()
            for chunk in unzipped_chunks:
                cont += bytearray(chunk)
            yield cont.decode()
