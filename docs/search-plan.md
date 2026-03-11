Core system design

Think of it as a hybrid legal retrieval + multi-agent answering stack.

A. Ingestion layer

From the dataset, ingest:

act/regulation title

citation

date

jurisdiction

language

full text

section text

source URL

amendment metadata if available

Then normalize into a canonical schema:

LegislativeDocument {
  doc_id
  jurisdiction
  instrument_type // act, regulation, order, rule, code
  title
  citation
  short_citation
  language
  date_enacted
  date_last_modified
  source_url
  full_text
  status // active, repealed, unknown
}

And a section-level table:

LegislativeSection {
  section_id
  doc_id
  section_number
  heading
  text
  parent_part
  parent_division
  parent_schedule
  language
  embeddings
  keywords
}
B. Chunking strategy

Do not chunk like generic RAG.

For legislation, chunk by:

section

subsection

paragraph

schedule

definition blocks

Also store structural hierarchy:

Part

Division

Section

Subsection

Paragraph

That gives you far better retrieval and answer citation.

C. Retrieval stack

Use three retrieval methods together:

BM25 / lexical search

best for exact statutory terms

catches defined terms, citations, and legal phrasing

Vector search

best for natural-language questions

good for concept-level matching

Graph / metadata filtering

jurisdiction

instrument type

date

language

domain

cited act

amended-by / related-to

This should be a hybrid ranker, not pure embeddings.

D. Agent fleet

You do not actually need “many agents scanning all legislation” on every query. That sounds cool, but it is expensive and often worse.

Better:

one router agent

one retrieval planner

a small number of domain specialist analyzers

one synthesis/citation agent

A clean query flow:

Intent classifier

question

compare

summarize

compliance lookup

definition lookup

find relevant laws

Domain router

privacy

labor

accessibility

housing

transportation

environment

taxation

Indigenous governance

criminal

immigration

etc.

Retriever

get top N sections + top N documents

Specialist analyzer agent(s)

read only retrieved material

produce issue-specific notes

Answer composer

forms answer

cites exact provisions

flags uncertainty

distinguishes answer vs source text vs interpretation

E. Output design

Every answer should include:

concise answer

relevant acts/regs

relevant sections

why these were selected

confidence

“not legal advice” boundary if public-facing

Ideal UI blocks:

Answer

Authorities cited

Relevant sections

Related legislation

Potential gaps / ambiguity

Why this works

Because legal users care about:

recall

exact citations

traceability

jurisdiction precision

currentness

They do not want a generic chatbot vibe.