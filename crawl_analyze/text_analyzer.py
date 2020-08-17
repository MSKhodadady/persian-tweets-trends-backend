from hazm import Lemmatizer, Normalizer, word_tokenize


async def text_analyze(text: str) -> list:
    normalizer = Normalizer()
    lemmatizer = Lemmatizer()

    text_norm = normalizer.normalize(text)

    token_list = tuple(
        word_tokenize(text_norm)
    )

    token_list_lem = tuple(map(
        lemmatizer.lemmatize,
        token_list
    ))

    return list(set(token_list_lem))
