#import pyspark
import pandas as pd
import numpy as np
import re
from ontario.rdfizer.mapping.RMLMapping import *

import rdflib


def applymapping(row, mappings, increment):
    result = []
    for m in mappings:
        submap = m.subjectmap
        subjmap = m.subjectmap.smap
        template = m.subjectmap.template
        subjrdfclass = m.subjectmap.rdfclass

        span, col = getplaceholder(template)
        start, end = span
        col = col[1:-1]

        autoinc = autoincrement(template)
        if autoinc:
            span = autoinc[0]
            increment[0] += 1
            subj = '<' + template[:start] + row[col] + template[end:span[0]] + str(increment[0]) + ">"
        else:
            subj = '<' + template[:start] + row[col] + template[end:] + ">"

        triple = subj + ' <http://www.w3.org/2000/01/rdf-schema#type> <' + subjrdfclass + '> . '
        if triple not in result:
            result.append(triple)

        predobjmap = m.predobjmap
        for predicate in predobjmap:
            maptype = predobjmap[predicate].maptype  # Const, Reference, Template, ParentTriplesMap
            objvalue = predobjmap[predicate].objvalue
            reference = predobjmap[predicate].reference
            termtype = predobjmap[predicate].termtype
            datatype = predobjmap[predicate].datatype
            language = predobjmap[predicate].language
            objtemplate = predobjmap[predicate].template
            parenttriplemap = predobjmap[predicate].parenttriplemap

            if maptype == 'Const':
                triple = subj + " <" + predicate + "> <" + objvalue + "> . "
                if triple not in result:
                    result.append(triple)
            if maptype == 'Reference':

                if datatype:
                    if datatype == "http://www.w3.org/2001/XMLSchema#string":
                        triple = subj + " <" + predicate + '> "' + row[reference] + '"^^<' + datatype + '> . '
                        if triple not in result:
                            result.append(triple)
                    elif datatype == "http://www.w3.org/2001/XMLSchema#int" or datatype == "http://www.w3.org/2001/XMLSchema#float":
                        triple = subj + " <" + predicate + '> "' + str(row[reference]) + '"^^<' + datatype + '> . '
                        if triple not in result:
                            result.append(triple)
                    elif datatype == "http://www.w3.org/2001/XMLSchema#anyURI":
                        triple = subj + " <" + predicate + "> <" + row[reference] + "> . "
                        if triple not in result:
                            result.append(triple)
                else:
                    triple = subj + " <" + predicate + "> <" + row[reference] + "> . "
                    if triple not in result:
                        result.append(triple)

            if maptype == 'Template':
                span, col = getplaceholder(objtemplate)
                start, end = span
                col = col[1:-1]

                autoinc = autoincrement(objtemplate)
                if autoinc:
                    span = autoinc[0]
                    obj = '<' + objtemplate[:start] + row[col] + objtemplate[end:span[0]] + str(increment[0]) + ">"
                else:
                    obj = '<' + objtemplate[:start] + row[col] + objtemplate[end:] + ">"

                triple = subj + " <" + predicate + "> " + obj + ' . '
                if triple not in result:
                    result.append(triple)

            if maptype == 'ParentTriplesMap':
                # TODO: check if psuri is not same as current sourceuri
                # assuming reference triples are in the same file
                span, col = getplaceholder(parenttriplemap.template)
                start, end = span
                col = col[1:-1]

                autoinc = autoincrement(parenttriplemap.template)
                if autoinc:
                    span = autoinc[0]
                    obj = '<' + parenttriplemap.template[:start] + row[col] + parenttriplemap.template[
                                                                              end:span[0]] + str(increment[0]) + ">"
                else:
                    obj = '<' + parenttriplemap.template[:start] + row[col] + parenttriplemap.template[end:] + ">"

                triple = subj + " <" + predicate + "> " + obj + ' . '
                if triple not in result:
                    result.append(triple)
    # result = '\n' .join(result)
    # print(result)
    return result


def getplaceholder(url):
    c = '(\{(.*?)\})'
    p = re.compile(c)
    # url = 'http://gdc.cancer.gov/schema/Sample/{Sample}'
    m = p.finditer(url)
    for a in m:
        return a.span(), a.group()


def autoincrement(url):
    c = '(\[(.*?)\])'
    p = re.compile(c)

    m = p.finditer(url)
    if m:
        for a in m:
            return a.span(), a.group()
    else:
        return None


def transform(f, sources):
    pdf = pd.read_csv(f, sep='\t')
    increment = [0]
    ttls = pdf.apply(applymapping, args=(sources, increment,), axis=1)
    return ttls


def get_rml_source(r, rmlsource=None):
    sid = str(r.s)
    source = str(r.source)
    referenceform = str(r.ref)
    iterator = r.iterator
    uri = r.sourceuri

    subjmap = r.smap
    template = r.template
    subjrdfclass = str(r.rdfclass)

    pomap = str(r.pomap)
    predicate = str(r.predicate)
    objmap = str(r.omap)

    objconstval = str(r.objval)
    objreference = str(r.oreference)
    objtemplate = str(r.objtemplate)

    objlang = str(r.lang)
    if r.datatype:
        objdtype = str(r.datatype)
    else:
        objdtype = None
    if r.termtype:
        objtermtype = str(r.termtype)
    else:
        objtermtype = None

    objparentmap = str(r.parentmap)
    pslsource = str(r.plsource)
    psuri = str(r.psourceuri)
    if r.psref:
        psrefformulation = str(r.psref)
    else:
        psrefformulation = None

    psiterator = str(r.psiterator)
    psmap = str(r.psmap)
    if r.psrdfclass:
        psrdfclass = str(r.psrdfclass)
    else:
        psrdfclass = None

    pstemplate = str(r.pstemplate)

    mapType = 'Reference'
    if r.objval:
        mapType = 'Const'
        rmlobject = RMLObjectMap(mapType, objvalue=objconstval, reference=None, termtype=objtermtype,
                                 datatype=objdtype,
                                 language=objlang, parenttriplemap=None)
    elif r.objtemplate:
        mapType = "Template"
        rmlobject = RMLObjectMap(mapType, template=objtemplate, objvalue=None, reference=None, termtype=objtermtype,
                                 datatype=objdtype, language=objlang, parenttriplemap=None)
    elif r.parentmap:
        mapType = 'ParentTriplesMap'
        psrmlsubject = RMLSubjectMap(objparentmap, psmap, pstemplate, psrdfclass)
        rmlobject = RMLObjectMap(mapType, parenttriplemap=psrmlsubject, objvalue=None, template=None,
                                 reference=None,
                                 termtype=objtermtype, datatype=objdtype, language=objlang)
    else:
        rmlobject = RMLObjectMap(mapType, reference=objreference, parenttriplemap=None, objvalue=None,
                                 template=None,
                                 termtype=objtermtype, datatype=objdtype, language=objlang)
    if not rmlsource:
        rmlsubject = RMLSubjectMap(sid, subjmap, template, subjrdfclass)
        rmlsource = RMLSource(sid, source, uri, referenceform, iterator)
        rmlsource.set_subject_map(rmlsubject)

    rmlsource.predobjmap[predicate] = rmlobject

    return rmlsource


def main(mappingfile):
    g = rdflib.Graph()
    g.load(mappingfile, format='n3')

    sourceres = g.query(sourcequery)

    rmlsources = {}
    for r in sourceres:
        sid = str(r.s)

        if sid in rmlsources:
            rmlsource = rmlsources[sid]
            rmlsource = get_rml_source(r, rmlsource)
        else:
            rmlsource = get_rml_source(r)
            rmlsources[sid] = rmlsource

    # get sources with common file name
    filesubj = {}
    for s in rmlsources:
        Source = rmlsources[s]
        if Source.uri in filesubj:
            filesubj[Source.uri].append(Source)
        else:
            filesubj[Source.uri] = [Source]
    # for each file name, make transform
    for f in filesubj:
        ttls = transform(f, filesubj[f])

        flat_list = [item for sublist in ttls for item in sublist]

        ttls = list(set(flat_list))
        ttls = "\n".join(ttls)

        # Assuming absolute path of a file is given, e.g., file:///data/name.txt
        fpath = f[f.find(':') + 3: f.rfind('/')]
        fname = f[f.rfind('/'):f.rfind('.')]
        with open(fpath + fname + '.nt', 'w+') as file:
            file.write(ttls)

sourcequery = '''
     prefix rr: <http://www.w3.org/ns/r2rml#> 
     prefix rml: <http://semweb.mmlab.be/ns/rml#> 
     prefix ql: <http://semweb.mmlab.be/ns/ql#> 
     SELECT DISTINCT *
     WHERE {                 
         ?s rml:logicalSource ?source .
         ?source rml:source ?sourceuri .
         ?source rml:referenceFormulation ?ref .
         OPTIONAL { ?source rml:iterator  ?iterator.}    

         ?s rr:subjectMap  ?smap. 
         ?smap rr:template ?template .
         ?smap rr:class ?rdfclass .

        ?s  rr:predicateObjectMap ?pomap .
        ?pomap rr:predicate ?predicate ;
        rr:objectMap ?omap.
        OPTIONAL{ ?omap rr:constant ?objval }
        OPTIONAL{ ?omap rml:reference ?oreference. }
        OPTIONAL{ 
                ?omap rr:parentTriplesMap ?parentmap .
                ?parentmap rr:subjectMap ?psmap .
                ?psmap rr:template ?pstemplate . 
                ?psmap rr:class ?psrdfclass .
                ?parentmap rml:logicalSource ?plsource .
                ?plsource rml:source ?psourceuri .
                ?plsource rml:referenceFormulation ?psref .
                optional {?plsource rml:iterator ?psiterator }
                }
        OPTIONAL{ ?omap rr:template ?objtemplate.}
        OPTIONAL{ ?omap rr:datatype ?datatype .}
        OPTIONAL{ ?omap rr:termType ?termtype .}
        OPTIONAL{ ?omap rr:language ?lang .}
     }
  '''


if __name__ == '__main__':
    mappingfile = "/home/dsdl/PycharmProjects/SparkRDFizer/config/tcgamapping.ttl"
    main(mappingfile)
