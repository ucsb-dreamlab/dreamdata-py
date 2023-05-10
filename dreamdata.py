import ocflindex
from stream_unzip import stream_unzip
from dataclasses import dataclass
from datetime import date
import xml.etree.ElementTree as ET

@dataclass
class NewsRecord:
    """NewsRecord represent a record from the ProQuest Historical Newspaper
    Collection. Fields are based on the xml schema from ProQuest."""
    version: str
    record_id: str
    datetimestamp: str
    record_title: str
    publication_id: str
    publication_title: str
    publication_qualifier: str
    publisher: str
    alpha_pubdate: str
    numeric_pubdate: str
    source_type: str
    object_type: str
    language_code: str
    issn: str | None
    start_page: int
    pagination: int
    url_docview: str
    fulltext: str


class Client(ocflindex.Client):
    """Client for accessing DREAM Lab data collection API."""

    def pqnews_getrecords(self, name: str, pubdate: date):
        """Used to access full text xml records from the ProQuest Historical
        Newspaper Collection. The method returns a generator yielding
        NewsRecords for the given publication and day. The publication names
        must be 'wapost', 'nytimes', or 'latimes')"""

        def resolveID(name: str, year: int, month:int, day:int) -> str:
            pubs = {
                "wapost":  [('18771206','60400'),('19230101','47009'),('19540318','47011'),('19590827','47012')],
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
        
        id = resolveID(name, pubdate.year, pubdate.month, pubdate.day)
        state = self.get_object_state(id, "xml.zip")
        
        for _, _, unzipped_chunks in stream_unzip(self.content_stream(state.digest)):
            cont = bytearray()
            for chunk in unzipped_chunks:
                cont += bytearray(chunk)
            root = ET.fromstring(cont.decode(encoding='utf-8'))
            pubElem = root.find("Publication")
            yield NewsRecord(version = root.findtext("Version"),
                record_id = root.findtext("RecordID"),
                datetimestamp = root.findtext("DateTimeStamp"),
                record_title = root.findtext("RecordTitle"),
                publication_id= pubElem.findtext("PublicationID"),
                publication_title = pubElem.findtext("Title"),
                publication_qualifier= pubElem.findtext("Qualifier"),
                publisher = root.findtext("Publisher"),
                alpha_pubdate = root.findtext("AlphaPubDate"),
                numeric_pubdate = root.findtext("NumericPubDat"),
                source_type = root.findtext("SourceType"),
                object_type = root.findtext("ObjectType"),
                language_code = root.findtext("LanguageCode"),
                issn = root.findtext("ISSN"),
                start_page = root.findtext("StartPage"),
                pagination = root.findtext("Pagination"),
                url_docview = root.findtext("URLDocView"),
                fulltext = root.findtext("FullText"))