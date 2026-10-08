import copy

import pytest

from kafka_hoard import doccompare as D
from kafka_hoard.hoard_link.agentkit import AppError
from conftest import tool
from pdfmaker import make_pdf


def pages(*texts): return [{'page':i+1,'text':t} for i,t in enumerate(texts)]


def test_shifted_page_and_repeated_rows_keep_real_amount_changes():
    left=pages('Cabecera\nTotal 100,00 EUR', 'Cabecera\nTotal 200,00 EUR')
    right=pages('Portada nueva', 'Cabecera\nTotal 100,00 EUR', 'Cabecera\nTotal 201,50 EUR')
    result=D.compare(left,right)
    assert [c['kind'] for c in result['changes']]==['insert','replace']
    change=result['changes'][1]
    assert D.excerpt(change['before'])['pages']==[2]
    assert D.excerpt(change['after'])['pages']==[3]
    assert '200,00' in D.excerpt(change['before'])['text'] and '201,50' in D.excerpt(change['after'])['text']


def test_numerals_signs_and_repeated_long_tables_not_ignored():
    left=pages('\n'.join(['Fila igual']*220+['Importe -2.25']))
    right=pages('\n'.join(['Fila igual']*220+['Importe -2.50']))
    r=D.compare(left,right);assert len(r['changes'])==1
    assert '-2.50' in D.excerpt(r['changes'][0]['after'])['text']
    assert D.compare(pages('A  B\n\nC'),pages('A B\nC'))['same_text'] is True
    assert D.compare(pages('A'),pages('a'))['same_text'] is False


def test_empty_text_is_unknown_instead_of_identical():
    assert D.compare(pages(''),pages(''))['same_text'] is None
    r=D.compare(pages('Title',''),pages('Title',''))
    assert r['same_text'] is None and r['empty_text_pages']['after']==[2]


def test_input_limit_and_excerpt_markers():
    with pytest.raises(ValueError,match='too many'):D.compare(pages(*['x']*201),pages('x'))
    with pytest.raises(ValueError,match='too much'):D.compare(pages('x'*(D.MAX_CHARS+1)),pages('x'))
    out=D.excerpt([(i,'é'*500,'') for i in range(1,22)])
    assert out['excerpt_truncated'] and out['pages_truncated'] and out['page_count']==21
    assert len(out['text'].encode('utf-8'))<=800


def test_real_archived_pdf_compare_citations_pagination_and_no_mutations(svc):
    ids=[]
    for texts in [('Head\nTotal 100 EUR','Clause old'),('Cover','Head\nTotal 125 EUR','Clause new')]:
        r=svc.engine.ingest_bytes(make_pdf(list(texts)), f'test-{len(ids)}.pdf', source='manual')
        ids.append(r['documents'][0]['id'])
    before=[svc.store.document(d) for d in ids]
    stored=[copy.deepcopy(svc.store.pages(d)) for d in ids]
    r=tool(svc,'doc_compare',before=ids[0],after=ids[1],limit=1)
    assert r['total_changes']>=2 and len(r['changes'])==1 and r['next_offset']==1
    r2=tool(svc,'doc_compare',before=ids[0],after=ids[1],offset=1)
    assert any('125' in c['after']['text'] for c in r2['changes'])
    assert all(c['before']['cites'] or c['after']['cites'] for c in r['changes']+r2['changes'])
    assert [svc.store.document(d) for d in ids]==before
    assert [svc.store.pages(d) for d in ids]==stored
    same=tool(svc,'doc_compare',before=ids[0],after=ids[0])
    assert same['same_text'] is True and same['changes']==[]
    with pytest.raises(AppError):tool(svc,'doc_compare',before=ids[0],after=ids[1],after_pages=[99])


def test_agent_masking_keeps_detected_differences_and_ui_can_reveal(svc):
    left=tool(svc,'doc_add_text',title='Before',text='Referencia\nDNI 12345678Z\nImporte 10 EUR')['documents'][0]['id']
    right=tool(svc,'doc_add_text',title='After',text='Referencia\nDNI 87654321X\nImporte 11 EUR')['documents'][0]['id']
    out=tool(svc,'doc_compare',before=left,after=right)
    assert out['same_text'] is False
    assert '12345678Z' not in str(out) and '[oculto: DNI]' in str(out)
    revealed=tool(svc,'doc_compare',before=left,after=right,reveal=True)
    assert '12345678Z' in str(revealed)


def test_masked_byte_budget_pagination_never_skips_a_change(svc):
    import json
    ids=[]
    for variant in ('before','after'):
        text='\n'.join(f'Anchor {i}\nValue {variant} {i} '+('12345678Z '*35) for i in range(70))
        ids.append(tool(svc,'doc_add_text',title=variant,text=text)['documents'][0]['id'])
    offset=0;seen=[]
    while True:
        out=tool(svc,'doc_compare',before=ids[0],after=ids[1],offset=offset,limit=30)
        assert 'truncated' not in out and len(json.dumps(out,ensure_ascii=False).encode('utf-8'))<20_000
        seen.extend(c['index'] for c in out['changes'])
        if out['next_offset'] is None:break
        assert out['next_offset']>offset
        offset=out['next_offset']
    assert seen==list(range(70))


def test_source_extraction_warnings_remain_visible_in_text_equality(svc):
    d=tool(svc,'doc_add_text',title='Stored prefix',text='Only this text is archived')['documents'][0]['id']
    doc=svc.store.document(d)
    svc.store.update_document(d,facts={**doc['facts'],'notes':['only the first 400 pages were read']})
    out=tool(svc,'doc_compare',before=d,after=d)
    assert out['same_text'] is True and out['equality_basis']=='selected_archived_text'
    assert out['original_files_compared'] is False
    assert 'first 400' in out['before']['extraction_notes'][0]


def test_identifier_is_masked_before_excerpt_can_cut_its_checksum_letter():
    from kafka_hoard.privacy import mask_text
    rows=[(1,'A'*795+' 12345678Z','')]
    result=D.excerpt(rows,transform_text=mask_text)
    assert '1234' not in result['text'] and result['excerpt_truncated']


def test_metadata_masking_precedes_title_and_extraction_note_shortening(svc):
    d=tool(svc,'doc_add_text',title='Synthetic',text='Archived text')['documents'][0]['id']
    doc=svc.store.document(d)
    svc.store.update_document(d,title='A'*155+' 12345678Z',
                              facts={**doc['facts'],'notes':['B'*195+' 12345678Z']})
    out=tool(svc,'doc_compare',before=d,after=d)
    assert '1234' not in str(out)
    assert out['before']['extraction_notes_truncated'] is True
    revealed=tool(svc,'doc_compare',before=d,after=d,reveal=True)
    assert '1234' in revealed['before']['title']
