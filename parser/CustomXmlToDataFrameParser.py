"""
  This function takes a string as input and returns a pySpark DataFrame
"""


import xml.etree.ElementTree as ET
from pyspark.sql import Row
from pyspark.sql.types import *

def parse(xmlString):
    tree = ET.ElementTree(ET.fromstring(xmlString.encode('utf-8')))
    root = tree.getroot()
    _id =root.attrib.get('Id')
    _tag =root.attrib.get('Tags')
    _title =root.attrib.get('Title')
    _body = root.attrib.get('Body')
    TAG_RE = re.compile(r'<[^>]+>')
    if(_title and _body and _tag and _title):
        _body=TAG_RE.sub('', _body).replace('\n',  ' ').replace('.', ' ')
        _title=TAG_RE.sub('', _title).replace('.', ' ').replace('\n', '')
        _text = (_title + " " +_body).replace(',', '').replace('?', '')
        _tags = _tag.replace('<','').replace('>','')
        return Row(_id, _tags, _text)
    else:
        return None
