class RMLSource(object):
    def __init__(self, sid, source, sourceuri, ref, iterator):
        self.sid = sid
        self.source = source
        self.uri = sourceuri
        self.ref = ref
        self.iterator = iterator
        self.subjectmap = None
        self.predobjmap = {}

    def __repr__(self):
        val = "<" + self.sid + "> \n" + \
              'rml:logicalSource [\n rml:source "' + \
              self.uri + '"; \n rml:referenceFormulation <' + \
              self.ref + ">; \n rml:iterator " + str(self.iterator) + "\n];" + \
              '\n\n' + str(self.subjectmap)
        for pred in self.predobjmap:
            val += "\n\n rr:predicateObjectMap [ \n\t rr:predicate " + pred + ";"
            val += str(self.predobjmap[pred])
            val += "\n];"
        return val[:-1] + "."

    def set_subject_map(self, subjectmap):
        self.subjectmap = subjectmap

    def set_predicate_object_map(self, predobjmap):
        self.predobjmap = predobjmap


class RMLSubjectMap(object):
    def __init__(self, sid, smap, template, rdfclass):
        self.sid = sid
        self.smap = smap
        self.template = template
        self.rdfclass = rdfclass

    def __repr__(self):
        return ' rr:subjectMap[ \n\t\t rr:template "' + self.template + '"; \n\t\t rr:class ' + self.rdfclass + '\n\t];'


class RMLObjectMap(object):
    def __init__(self, maptype, objvalue=None, reference=None, template=None, termtype=None, datatype=None,
                 language=None, parenttriplemap=None):

        self.maptype = maptype  # Const, Reference, Template, ParentTriplesMap
        self.objvalue = objvalue
        self.reference = reference
        self.termtype = termtype
        self.datatype = datatype
        self.language = language
        self.template = template
        self.parenttriplemap = parenttriplemap

    def __repr__(self):
        if self.maptype == 'Const':
            return '\n\t rr:objectMap [ \n\t\t rr:constant ' + self.objvalue + "\n\t ]"
        if self.maptype == 'Reference':
            return '\n\t rr:objectMap [ \n\t\t rml:reference "' + self.reference + '"\n\t ]'
        if self.maptype == 'Template':
            return '\n\t rr:objectMap [ \n\t\t rr:template "' + self.template + '"\n\t ]'
        if self.maptype == 'ParentTriplesMap':
            return '\n\t rr:objectMap [ \n\t\t rr:parentTriplesMap [\n\t\t ' + str(
                self.parenttriplemap) + "\n\t\t]\n\t ]"
