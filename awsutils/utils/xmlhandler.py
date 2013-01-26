# awsutils/utils/xmlhandler.py
# Copyright 2013 Sandor Attila Gerendi (Sanyi)
#
# This module is part of awsutils and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from xml.sax import ContentHandler

class AWSXMLHandler(ContentHandler):
    def __init__(self):
        self.xml = None
        self.stack = [None]
        ContentHandler.__init__(self)

    def reset(self):
        self.xml = None
        self.stack = [None]

    def getdict(self):
        return self.xml

    def startElement(self, name, attrs):
        self.stack.append(([], []))

    def characters(self, content):
        content = content.strip()
        if content != '':
            self.stack[-1][1].append(content)

    def endElement(self, name):
        current_element = self.stack.pop()
        current_element_children = current_element[0]
        current_element_data = current_element[1]

        element = {}

        # if no children then the value is the data
        if current_element_children == []:
            element = ''.join(current_element_data)
        else:
            for child in current_element_children:
                key = child[0]
                value = child[1]
                if not key in element:
                    element[key] = [value]
                else:
                    element[key].append(value)

                if current_element_data != []:
                    element['data'] = "".join(current_element_data).strip()

            for key in element.keys():
                if len(element[key]) == 1:
                    element[key] = element[key][0]

        if len(self.stack) > 1:
            parrent_element_children = self.stack[-1][0]
            parrent_element_children.append((name, element))
        else:
            self.xml = {name: element}

        current_element = None