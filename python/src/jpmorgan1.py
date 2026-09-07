from typing import List

# ============================================================
#  YOU IMPLEMENT THIS.
#  Perform ONE BPE merge step:
#    1. count adjacent pairs (overlapping) across all words
#    2. pick the highest-frequency pair; ties broken by
#       lexicographic order of the pair (first, then second)
#    3. in each word, left-to-right, merge that pair;
#       after a merge the scan pointer skips past the new symbol
#       (so overlaps collapse to one merge)
#  Return the new corpus.  If no pair exists (all words len<2),
#  return corpus unchanged.
# ============================================================
def bpe_merge_step(corpus: List[List[str]]) -> List[List[str]]:
    # TODO: your code here
    fre ={}
    max_c =0
    max_term =""
    for doc in corpus:
        if len(doc)==0:
            continue
        for i in range(len(doc)-1):
            merge =doc[i]+doc[i+1]
            if merge not in fre:
                fre[merge] =0
            fre[merge] = fre[merge] +1
            if fre[merge] > max_c or (fre[merge] == max_c and max_term > merge):
                max_c =fre[merge]
                max_term = merge
    new_corpus =[]
    for doc in corpus:
        if len(doc) ==0:
            new_corpus.append(doc)
            continue
        new_doc=[]
        i=0
        while i < len(doc)-1:
            merge= doc[i]+doc[i+1]
            if merge == max_term:
                new_doc.append(merge)
                i+=2
            else:
                new_doc.append(doc[i])
                i+=1
        if i==len(doc)-1:
            new_doc.append(doc[i])
        new_corpus.append(new_doc)
    return new_corpus


# ============================================================
#  Test harness — do not edit below.
# ============================================================
def _run():
    passed = failed = 0
    def check(name, corpus, expected):
        nonlocal passed, failed
        got = bpe_merge_step([list(w) for w in corpus])
        if got == expected:
            passed += 1
            print(f"  [PASS] {name} -> {got}")
        else:
            failed += 1
            print(f"  [FAIL] {name}")
            print(f"         got      {got}")
            print(f"         expected {expected}")

    # 1) spec example: ('e','s')=2, ('s','t')=2 tie -> lexicographically ('e','s') wins
    check("spec_es_wins",
          [['l','o','w'],
           ['l','o','w','e','r'],
           ['n','e','w','e','s','t'],
           ['w','i','d','e','s','t']],
          [['l','o','w'],
           ['l','o','w','e','r'],
           ['n','e','w','es','t'],
           ['w','i','d','es','t']])

    # 2) overlap: ('a','a') counted twice in 'aaa' but merges once -> ['aa','a']
    check("overlap_aaa",
          [['a','a','a']],
          [['aa','a']])

    # 3) overlap longer: 'aaaa' -> left-to-right: merge[0,1]->aa, skip, merge[2,3]->aa
    check("overlap_aaaa",
          [['a','a','a','a']],
          [['aa','aa']])

    # 4) clear winner, multiple words
    check("clear_winner",
          [['a','b','c'],
           ['a','b','d'],
           ['x','a','b']],
          [['ab','c'],
           ['ab','d'],
           ['x','ab']])

    # 5) tie broken by lexicographic pair order: ('a','b') vs ('c','d') both freq 1 -> ('a','b')
    check("tie_lex",
          [['a','b'],
           ['c','d']],
          [['ab'],
           ['c','d']])

    # 6) no mergeable pair (all single-symbol words) -> unchanged
    check("no_pairs",
          [['a'], ['b'], ['c']],
          [['a'], ['b'], ['c']])

    # 7) empty corpus
    check("empty_corpus",
          [],
          [])

    # 8) merge creates a 3-char symbol later, but THIS step only does one merge
    #    ('l','o')=2 highest -> only lo merged this step
    check("single_step_only",
          [['l','o','w'],
           ['l','o','w']],
          [['lo','w'],
           ['lo','w']])

    print(f"\n==== {passed} passed, {failed} failed ====")
    return failed == 0

if __name__ == "__main__":
    import sys
    sys.exit(0 if _run() else 1)