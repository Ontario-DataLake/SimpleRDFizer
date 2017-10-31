# Simple RDFizer
SimpleRDFizer tool transforms TSV/CSV files to RDF (N-Triple) format using a given RML mapping.

- to run RDFizer
```sh
     $ python rdfizer.py -m <path-to-rml-mapping-file>
```

- sample mapping file can be found in ```config``` folder

``` 
    #Chromosome
    <#chromosome>
    rml:logicalSource [
        rml:source "file:///home/kemele/git/SimpleRDFizer/sample-data/BROSE_p_TCGASNP_195_196_197_N_GenomeWideSNP_6_F12_1039594.grch38.seg.txt";
        rml:referenceFormulation ql:TSV
    ];
    
    rr:subjectMap [
      rr:template "http://gdc.cancer.gov/schema/Chormosome/{Chromosome}";
      rr:class tcga:Chromosome
    ];
    
    rr:predicateObjectMap [
      rr:predicate rdfs:label;
      rr:objectMap [
        rml:reference "Chromosome";
        rr:datatype xsd:string
        ]
    ].
```