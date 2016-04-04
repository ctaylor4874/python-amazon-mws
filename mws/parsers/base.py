from lxml import etree


def first_element_or_none(element_list):
    """
    Return the first element or None from an lxml selector result.
    :param element_list: lxml selector result
    :return:
    """
    if element_list:
        return element_list[0]
    return


def first_element(f):
    """
    function wrapper for _first_element_or_none.

    This is equivalent to using `return _first_element_or_none(xpath())`.
    :param f:
    :return:
    """
    def inner(*args, **kwargs):
        return first_element_or_none(f(*args, **kwargs))
    return inner


class BaseElementWrapper(object):

    def __init__(self, element):
        self.element = element

    def __str__(self):
        return etree.tostring(self.element)


class BaseResponseMixin(object):

    @classmethod
    def load_from_file(cls, file_location):
        """
        Create an instance of this class from a file.

        :param file_location: path to file.
        :return:
        """
        with open(file_location, 'rb') as f:
            return cls.load(f.read())

    @classmethod
    def load(cls, xml_string):
        """
        Create an instance of this class using an xml string.

        :param xml_string:
        :return:
        """
        tree = etree.fromstring(xml_string)
        return cls(tree)
