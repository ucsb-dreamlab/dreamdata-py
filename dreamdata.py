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

    def pqnews_byid(self, id: str):
        """Returns a generator yielding NewsRecords for the resource id."""
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
                numeric_pubdate = root.findtext("NumericPubDate"),
                source_type = root.findtext("SourceType"),
                object_type = root.findtext("ObjectType"),
                language_code = root.findtext("LanguageCode"),
                issn = root.findtext("ISSN"),
                start_page = root.findtext("StartPage"),
                pagination = root.findtext("Pagination"),
                url_docview = root.findtext("URLDocView"),
                fulltext = root.findtext("FullText"))

    def pqnews_namedate(self, name: str, pubdate: date):
        """Used to access full text xml records from the ProQuest Historical
        Newspaper Collection. The method returns a generator yielding
        NewsRecords for the given publication and day. The publication names
        must be 'wapost', 'nytimes', or 'latimes')"""
        pubs = {
            "wapost":  [('1877/12/06','60400'),('1923/01/01','47009'),('1954/03/18','47011'),('1959/08/27','47012')],
            "nytimes": [('1851/09/18','11558'),('1857/09/14','55428'),('1923/01/01','45545')],
            "latimes": [('1881/12/04','46998'),('1886/10/23','55410'),('1923/01/01','47001')]
        }
        datestr = f"{pubdate.year:04}/{pubdate.month:02}/{pubdate.day:02}"
        pubid = ''
        for dateid in pubs[name]:
            if datestr > dateid[0]:
                pubid = dateid[1]
                continue
            if datestr < dateid[0]:
                break
            if datestr == dateid[0]:
                return dateid[1]        
        id = f"{pubid}/{datestr}"
        return self.pqnews_byid(id)
        