### Introduction

The hypothesis generation service is one of the analysis of pipelines n Rejuve.BIO platform. Its main goal is to allow users to do an end-to-end analysis of Genome Wide Association Study (GWAS) experiments to connect identify and connect variants to the phenotype under study via genes and pathways there by providing a complete picture of how a variant might potentially cause about an effect. It includes to four subcomponents:

- Finemapping
- Causal Gene Prediction
- Cell type annotation
- Pathway Enrichment

Below, I'll briefly explain each component of the hypothesis generation (HG) service and describe how each of them fit into the entire analysis pipeline.

Note: This document should be treated as living document and parts of it might change as the service evolves.

### Finemapping

Fine-mapping is a statistical method for identifying potential causal variants from genetic association signals such as those coming from GWAS studies [2] (citation-susie paper). The process of finding likely causal variant is difficult problem because of two main reasons:- a) more than 90% of GWAS variants are found in non-coding regions ("dark matter of the DNA") of the genome [1](citation) making direct discovery hard. In addition, many variatns in the genome show strong correlation patterns with other nearby variants (a phenomena known as [linkage disequilibrium]()). This makes fine-mapping difficult because a non-causal "tag" variant can look as significant as the true casual variant.

There are many fine-mapping tools with different modelling assumptions (such as bayesian vs likelihood based) and input types data (individual-level genotype vs summary statistics)

| Tool                  | Modeling assumption                                                      | Input type                         | Multi-causal | Typical outputs                      | Reference                                                                                                                                                                 |
| --------------------- | ------------------------------------------------------------------------ | ---------------------------------- | ------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SuSiE-RSS (susie_rss) | Bayesian sum of single effects (Variational Bayes)                       | Summary stats (z/beta) + LD + N    | Yes          | PIP, credible sets, effects          | SuSiE method: Zou et.al  22 <https://doi.org/10.1371/journal.pgen.1010299>; susie_rss vignette <https://stephenslab.github.io/susieR/articles/susie_rss.html> |
| SuSiE (susieR)        | Bayesian sum of single effects (variational Bayes)                       | Individual-level X,y (or XtX, Xty) | Yes          | PIP, credible sets, effects          | Wang et al., 2020  <https://doi.org/10.1111/rssb.12388>                                                                                                |
| FINEMAP               | Bayesian variable selection (stochastic search)                          | Summary z + LD                     | Yes          | PIP, credible sets, model posteriors | Benner et al., 2016 (Bioinformatics) <https://doi.org/10.1093/bioinformatics/btw018>                                                                                        |
| CAVIAR                | Bayesian causal set with LD                                              | Summary z + LD                     | Yes          | PIP, causal set probabilities        | Hormozdiari et al., 2014 (Nat Genet) <https://doi.org/10.1534/genetics.114.167908>                                                                                            |
| CAVIARBF              | Bayesian approximate Bayes factors with LD                               | Summary z + LD                     | Yes          | PIP, credible sets                   | Chen et al., 2015 (Genet Epidemiol) <https://doi.org/10.1534/genetics.115.176107>                                                                                                    |
| DAP-G                 | Bayesian hierarchical fine-mapping (deterministic approx. of posteriors) | Summary z + LD                     | Yes          | SNP PIP, signal PIP, credible sets   | Wen et al., 2016 (AJHG) <https://doi.org/10.1016/j.ajhg.2016.03.029>                                                                                                        |
| PAINTOR               | Bayesian fine-mapping integrating functional annotations                 | Summary z + LD + annotations       | Yes          | PIP, credible sets                   | Kichaev et al., 2014 (AJHG) <https://doi.org/10.1093/bioinformatics/btw615>                                                                                                    |

The current version of HG service uses SuSiE-RSS for fine-mapping. We primarly chose SuSiE because of its summary stat support and performance over existing methods as documented in [Zhou et.al](). SuSiE-RSS outputs a list of credible sets. A credible set is the smallest set of variants that contains a true causal variant with a probability $\alpha$ (usually 0.95), where $\alpha$ is the sum of the posterior inclusion probabilities (PIPs) of the SNPs in the credible set.

Before performing fine-mapping the HG services applies various preprocessings steps such as harmonization of the input summary stats data and identification of independent loci (ref COJO).

Once we identify a list of potential causal variants,we'd like to find which genes and pathways they influence and in which cell or tissue types the effects are likely active.These steps following fine-mapping address these questions

### Causal Gene Prediction

(TODO - include overview various causal gene prioritization methods)

For causal gene prediction, the HG service uses a probabilistic reasoning engine and an expert-curated biomedical knowledge graph (BioAtomspace). The reasoning is performed on the BioAtomspace using "soft" (probabilistic) rules and outputs candidate causal genes ranked by their probability of being causal. The ranking is done using evidence in the BioAtomspace, which contains (at the current version - needs to be updated) 600M nodes and 4B edges sourced from publicly available databases (TODO: add a link to the list of databases).

To learn the rule weights and test the performance of the reasoning engine, we used the [OpenTargets GWAS Gold Standards](https://github.com/opentargets/genetics-gold-standards) benchmark dataset. Briefly, the dataset contains over 400 GWAS loci with known gene-causal associations which serve as "ground-truth" causal associations for training and testing the causal gene prediction. The variant-gene associations are selected based on multiple lines of evidence including:

- Expert domain knowledge of strong orthogonal evidence
- Known drug target-disease pairs
- Experimental/observational functional data (e.g., epigenetic marks, colocalizing molQTLs)

We tested the current version of the causal gene prediction component on a version of the dataset restricted to variants (and their related KG) on chromosome 16 (chr16). Limiting the benchmark data to only chr16 resulted in 107 samples of variant-gene links. For each variant-gene link in the benchmark data (which we treat as positive samples), we generated a list of negative samples by creating a link between a variant and all the genes within 500kb of this variant. We used 5-fold cross-validation to measure the performance of the prediction and used 80% of the samples for training and 20% for testing. We used the Area Under the Curve (AUC) metric to measure the performance. The AUC represents the area under the Receiver Operating Characteristic (ROC) curve and ranges from 0 to 1, where 0.5 indicates random performance and 1.0 indicates perfect classification. The AUC can be interpreted as the probability that the model will rank a randomly chosen positive (true causal) variant-gene link higher than a randomly chosen negative (non-causal) variant-gene link. See the plot below for the performance results. The current system achieves an average AUC of 0.79 over the 5-fold cross-validation




### Cell type annotation



### Pathway Enrichment
