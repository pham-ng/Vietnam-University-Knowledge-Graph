# Knowledge Graph Validation and Evaluation Queries

Use these prefixes for all queries:

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX vio: <http://vi.dbpedia.org/ontology/>
PREFIX vres: <http://vi.dbpedia.org/resource/>
PREFIX wd: <http://www.wikidata.org/entity/>
```

## Validation Queries

### 1. Universities without any site

```sparql
SELECT ?u
WHERE {
  ?u rdf:type vio:University .
  FILTER NOT EXISTS { ?u vio:hasSite ?s }
}
ORDER BY ?u
```

### 2. Sites without latitude

```sparql
SELECT ?s
WHERE {
  ?s rdf:type vio:Site .
  FILTER NOT EXISTS { ?s geo:lat ?lat }
}
ORDER BY ?s
```

### 3. Sites without longitude

```sparql
SELECT ?s
WHERE {
  ?s rdf:type vio:Site .
  FILTER NOT EXISTS { ?s geo:long ?long }
}
ORDER BY ?s
```

### 4. Membership hierarchy

```sparql
SELECT ?u ?parent
WHERE {
  ?u vio:isMemberOf ?parent
}
ORDER BY ?parent ?u
```

### 5. Membership parent without valid type

```sparql
SELECT ?u ?parent
WHERE {
  ?u vio:isMemberOf ?parent .
  FILTER NOT EXISTS { ?parent rdf:type vio:University }
  FILTER NOT EXISTS { ?parent rdf:type vio:UniversitySystem }
}
ORDER BY ?u
```

## Competency Queries

### 1. Universities located in Hanoi

```sparql
SELECT DISTINCT ?u ?label
WHERE {
  ?u rdf:type vio:University ;
     vio:hasSite ?s .
  ?s vio:locatedInCity vres:Hanoi .
  OPTIONAL { ?u rdfs:label ?label }
}
ORDER BY ?label
```

### 2. Provinces with the most universities

```sparql
SELECT ?province (COUNT(DISTINCT ?u) AS ?universityCount)
WHERE {
  ?u rdf:type vio:University ;
     vio:hasSite ?s .
  ?s vio:locatedInProvince ?province .
}
GROUP BY ?province
ORDER BY DESC(?universityCount)
```

### 3. Universities governed by the Ministry of Defense

```sparql
SELECT DISTINCT ?u ?label
WHERE {
  ?u rdf:type vio:University ;
     vio:governedBy vres:Ministry_of_Defense .
  OPTIONAL { ?u rdfs:label ?label }
}
ORDER BY ?label
```

### 4. Universities belonging to Vietnam National University systems

```sparql
SELECT ?u ?label ?system
WHERE {
  ?u rdf:type vio:University ;
     vio:isMemberOf ?system .
  ?system rdf:type vio:UniversitySystem .
  FILTER (?system IN (vres:VNU_Hanoi, vres:VNU_HCM))
  OPTIONAL { ?u rdfs:label ?label }
}
ORDER BY ?system ?label
```

### 5. Universities with more than 10,000 students

```sparql
SELECT ?u ?label ?students
WHERE {
  ?u rdf:type vio:University ;
     vio:numberOfStudents ?students .
  FILTER (?students > 10000)
  OPTIONAL { ?u rdfs:label ?label }
}
ORDER BY DESC(?students)
```

### 6. Universities founded before 1950

```sparql
SELECT ?u ?label ?year
WHERE {
  ?u rdf:type vio:University ;
     vio:foundingYearOrg ?year .
  FILTER (?year < "1950"^^xsd:gYear)
  OPTIONAL { ?u rdfs:label ?label }
}
ORDER BY ?year
```

### 7. Universities with multiple campuses

```sparql
SELECT ?u ?label (COUNT(?site) AS ?siteCount)
WHERE {
  ?u rdf:type vio:University ;
     vio:hasSite ?site .
  OPTIONAL { ?u rdfs:label ?label }
}
GROUP BY ?u ?label
HAVING (COUNT(?site) > 1)
ORDER BY DESC(?siteCount)
```

### 8. Governing bodies and the number of universities they manage

```sparql
SELECT ?gov ?label (COUNT(DISTINCT ?u) AS ?universityCount)
WHERE {
  ?u rdf:type vio:University ;
     vio:governedBy ?gov .
  OPTIONAL { ?gov rdfs:label ?label }
}
GROUP BY ?gov ?label
ORDER BY DESC(?universityCount)
```

### 9. Cities hosting universities from more than one governing body

```sparql
SELECT ?city (COUNT(DISTINCT ?gov) AS ?governingBodyCount)
WHERE {
  ?u rdf:type vio:University ;
     vio:governedBy ?gov ;
     vio:hasSite ?site .
  ?site vio:locatedInCity ?city .
}
GROUP BY ?city
HAVING (COUNT(DISTINCT ?gov) > 1)
ORDER BY DESC(?governingBodyCount)
```

### 10. Universities with Wikidata links

```sparql
SELECT ?u ?qid ?sameAs
WHERE {
  ?u rdf:type vio:University ;
     vio:hasWikidataID ?qid ;
     owl:sameAs ?sameAs .
}
ORDER BY ?u
```

### 11. Heads of universities in Hanoi

```sparql
SELECT DISTINCT ?u ?universityLabel ?person ?personLabel
WHERE {
  ?u rdf:type vio:University ;
     vio:headOfUniversity ?person ;
     vio:hasSite ?site .
  ?site vio:locatedInCity vres:Hanoi .
  OPTIONAL { ?u rdfs:label ?universityLabel }
  OPTIONAL { ?person rdfs:label ?personLabel }
}
ORDER BY ?universityLabel
```

### 12. Universities grouped by city

```sparql
SELECT ?city (COUNT(DISTINCT ?u) AS ?universityCount)
WHERE {
  ?u rdf:type vio:University ;
     vio:hasSite ?site .
  ?site vio:locatedInCity ?city .
}
GROUP BY ?city
ORDER BY DESC(?universityCount)
```

## Graph Statistics Queries

### 1. Triple count

```sparql
SELECT (COUNT(*) AS ?tripleCount)
WHERE { ?s ?p ?o }
```

### 2. Number of universities

```sparql
SELECT (COUNT(DISTINCT ?u) AS ?universityCount)
WHERE { ?u rdf:type vio:University }
```

### 3. Number of sites

```sparql
SELECT (COUNT(DISTINCT ?s) AS ?siteCount)
WHERE { ?s rdf:type vio:Site }
```

### 4. Number of cities

```sparql
SELECT (COUNT(DISTINCT ?c) AS ?cityCount)
WHERE { ?c rdf:type vio:City }
```

### 5. Number of provinces

```sparql
SELECT (COUNT(DISTINCT ?p) AS ?provinceCount)
WHERE { ?p rdf:type vio:Province }
```

### 6. Number of governing bodies

```sparql
SELECT (COUNT(DISTINCT ?g) AS ?governingBodyCount)
WHERE { ?g rdf:type vio:GoverningBody }
```
