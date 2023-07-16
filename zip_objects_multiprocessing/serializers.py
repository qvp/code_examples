from abc import ABC, abstractmethod
import xml.etree.ElementTree as XmlElementTree

from schemas import Obj


class ObjSerializer(ABC):
    @abstractmethod
    def serialize(self, obj: Obj) -> str:
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, obj: str) -> Obj:
        raise NotImplementedError


class ObjXmlSerializer(ObjSerializer):
    def serialize(self, obj: Obj) -> str:
        xml_root = XmlElementTree.Element('root')

        for var_name, var_value in obj.vars.items():
            XmlElementTree.SubElement(xml_root, 'var', name=var_name, value=var_value)

        xml_objects = XmlElementTree.SubElement(xml_root, 'objects')
        for obj_name in obj.objects:
            XmlElementTree.SubElement(xml_objects, 'object', name=obj_name)

        return XmlElementTree.tostring(xml_root, encoding='unicode')

    def deserialize(self, obj_xml: str) -> Obj:
        xml_root = XmlElementTree.fromstring(obj_xml)

        vars_ = {}
        for var_tag in xml_root.findall('var'):
            vars_[var_tag.attrib['name']] = var_tag.attrib['value']

        objects = []
        if xml_objects := xml_root.find('objects'):
            for obj_tag in xml_objects.findall('object'):
                objects.append(obj_tag.get('name'))

        return Obj(vars_, objects)
