# Stopwords de NLTK (nltk.corpus.stopwords), las que usa el notebook de referencia
ENGLISH = set("""
a about above after again against ain all am an and any are aren as at be
because been before being below between both but by can couldn d did didn do
does doesn doing don down during each few for from further had hadn has hasn
have haven having he her here hers herself him himself his how i if in into is
isn it its itself just ll m ma me mightn more most mustn my myself needn no nor
not now o of off on once only or other our ours ourselves out over own re s
same shan she should shouldn so some such t than that the their theirs them
themselves then there these they this those through to too under until up ve
very was wasn we were weren what when where which while who whom why will with
won wouldn y you your yours yourself yourselves
""".split())

FRENCH = set("""
au aux avec ce ces dans de des du elle en et eux il ils je la le les leur lui
ma mais me même mes moi mon ne nos notre nous on ou par pas pour qu que qui sa
se ses son sur ta te tes toi ton tu un une vos votre vous c d j l à m n s t y
été étée étées étés étant étante étants étantes suis es est sommes
êtes sont serai seras sera serons serez seront serais serait serions seriez
seraient étais était étions étiez étaient fus fut fûmes fûtes furent
sois soit soyons soyez soient fusse fusses fût fussions fussiez fussent ayant
ayante ayantes ayants eu eue eues eus ai as avons avez ont aurai auras aura
aurons aurez auront aurais aurait aurions auriez auraient avais avait avions
aviez avaient eut eûmes eûtes eurent aie aies ait ayons ayez aient eusse
eusses eût eussions eussiez eussent
""".split())

ALL = ENGLISH | FRENCH