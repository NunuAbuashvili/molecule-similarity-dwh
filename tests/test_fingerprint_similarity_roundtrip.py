"""
Cross-module contract test for the silver -> gold fingerprint seam.

The packed bytes produced by include/fingerprint_silver.compute_fingerprint
must reconstruct, via include/similarity_gold.bytes_to_bitvect, into a bit
vector equivalent to a freshly computed RDKit Morgan fingerprint. Pins the
byte layout so a change on either side can't silently break similarity.
"""
from rdkit import Chem, DataStructs
from rdkit.Chem import rdFingerprintGenerator

from include.fingerprint_silver.fingerprints import compute_fingerprint
from include.similarity_gold.similarity import bytes_to_bitvect


class TestFingerprintRoundTrip:
    def test_self_tanimoto_is_one(self):
        bv = bytes_to_bitvect(compute_fingerprint("CCO"))  # ethanol
        assert DataStructs.TanimotoSimilarity(bv, bv) == 1.0

    def test_reconstructed_bits_match_source_generator(self):
        smiles = "c1ccccc1"  # benzene
        bv = bytes_to_bitvect(compute_fingerprint(smiles))

        generator = rdFingerprintGenerator.GetMorganGenerator(
            radius=2, fpSize=2048
        )
        reference = generator.GetFingerprint(Chem.MolFromSmiles(smiles))

        assert list(bv.GetOnBits()) == list(reference.GetOnBits())
        assert DataStructs.TanimotoSimilarity(bv, reference) == 1.0

    def test_different_molecules_score_below_one(self):
        ethanol = bytes_to_bitvect(compute_fingerprint("CCO"))
        benzene = bytes_to_bitvect(compute_fingerprint("c1ccccc1"))
        sim = DataStructs.TanimotoSimilarity(ethanol, benzene)
        assert 0.0 <= sim < 1.0
