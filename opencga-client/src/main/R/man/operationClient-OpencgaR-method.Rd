% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Operation-methods.R
\name{operationClient,OpencgaR-method}
\alias{operationClient,OpencgaR-method}
\title{OperationClient methods}
\usage{
\S4method{operationClient}{OpencgaR}(OpencgaR, endpointName, params = NULL, ...)
}
\arguments{
\item{annotationUpdate}{Create and load variant annotations into the database.}

\item{annotationSaveId}{Save a copy of the current variant annotation at the database.}

\item{annotationId}{Annotation identifier.}

\item{skipRebuild}{Skip sample index re-build.}

\item{name}{Unique name of the score within the study.}

\item{resume}{Resume a previously failed remove.}

\item{force}{Force remove of partially indexed scores.}

\item{project}{Project \link{user@}project where project can be either the ID or the alias.}

\item{samples}{Samples to remove. Needs to provide all the samples in the secondary index.}

\item{jobId}{Job ID. It must be a unique string within the study. An ID will be autogenerated automatically if not provided.}

\item{jobDescription}{Job description.}

\item{jobDependsOn}{Comma separated list of existing job IDs the job will depend on.}

\item{jobTags}{Job tags.}

\item{study}{Study [\link{user@}project:]study where study and project can be either the ID or UUID.}

\item{data}{Variant delete study params.}
}
\description{
This function implements the OpenCGA calls for managing Operations - Variant Storage.
The following table summarises the available \emph{actions} for this client:\tabular{llr}{
   endpointName \tab Endpoint WS \tab parameters accepted \cr
   configureCellbase \tab /{apiVersion}/operation/cellbase/configure \tab project, annotationUpdate, annotationSaveId, body \cr
   aggregateVariant \tab /{apiVersion}/operation/variant/aggregate \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
   deleteVariantAnnotation \tab /{apiVersion}/operation/variant/annotation/delete \tab jobId, jobDescription, jobDependsOn, jobTags, project, annotationId \cr
   indexVariantAnnotation \tab /{apiVersion}/operation/variant/annotation/index \tab jobId, jobDescription, jobDependsOn, jobTags, project, study, body \cr
   saveVariantAnnotation \tab /{apiVersion}/operation/variant/annotation/save \tab jobId, jobDescription, jobDependsOn, jobTags, project, body \cr
   configureVariant \tab /{apiVersion}/operation/variant/configure \tab project, study, body \cr
   deleteVariant \tab /{apiVersion}/operation/variant/delete \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
   aggregateVariantFamily \tab /{apiVersion}/operation/variant/family/aggregate \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
   indexVariantFamily \tab /{apiVersion}/operation/variant/family/index \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
   indexVariant \tab /{apiVersion}/operation/variant/index \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
   launcherVariantIndex \tab /{apiVersion}/operation/variant/index/launcher \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
   runVariantJulie \tab /{apiVersion}/operation/variant/julie/run \tab jobId, jobDescription, jobDependsOn, jobTags, project, body\link{*} \cr
   repairVariantMetadata \tab /{apiVersion}/operation/variant/metadata/repair \tab jobId, jobDescription, jobDependsOn, jobTags, body \cr
   synchronizeVariantMetadata \tab /{apiVersion}/operation/variant/metadata/synchronize \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
   deleteVariantSample \tab /{apiVersion}/operation/variant/sample/delete \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
   indexVariantSample \tab /{apiVersion}/operation/variant/sample/index \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
   variantSampleIndexConfigure \tab /{apiVersion}/operation/variant/sample/index/configure \tab study, skipRebuild, body \cr
   deleteVariantScore \tab /{apiVersion}/operation/variant/score/delete \tab jobId, jobDescription, jobDependsOn, jobTags, study, name, resume, force \cr
   indexVariantScore \tab /{apiVersion}/operation/variant/score/index \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
   secondaryIndexVariant \tab /{apiVersion}/operation/variant/secondaryIndex \tab jobId, jobDescription, jobDependsOn, jobTags, project, study, body \cr
   deleteVariantSecondaryIndex \tab /{apiVersion}/operation/variant/secondaryIndex/delete \tab jobId, jobDescription, jobDependsOn, jobTags, study, samples \cr
   deleteVariantStats \tab /{apiVersion}/operation/variant/stats/delete \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   indexVariantStats \tab /{apiVersion}/operation/variant/stats/index \tab study, jobId, jobDescription, jobDependsOn, jobTags, body\link{*} \cr
   deleteVariantStudy \tab /{apiVersion}/operation/variant/study/delete \tab jobId, jobDescription, jobDependsOn, jobTags, study, body \cr
}
}
\section{Endpoint /{apiVersion}/operation/cellbase/configure}{

Update Cellbase configuration.
}

\section{Endpoint /{apiVersion}/operation/variant/aggregate}{

Find variants where not all the samples are present, and fill the empty values, excluding HOM-REF (0/0) values.
}

\section{Endpoint /{apiVersion}/operation/variant/annotation/delete}{

Deletes a saved copy of variant annotation.
}

\section{Endpoint /{apiVersion}/operation/variant/annotation/index}{

Create and load variant annotations into the database.
}

\section{Endpoint /{apiVersion}/operation/variant/annotation/save}{

Save a copy of the current variant annotation at the database.
}

\section{Endpoint /{apiVersion}/operation/variant/configure}{

Update Variant Storage Engine configuration. Can be updated at Project or Study level.
}

\section{Endpoint /{apiVersion}/operation/variant/delete}{

Remove variant files from the variant storage.
}

\section{Endpoint /{apiVersion}/operation/variant/family/aggregate}{

Find variants where not all the samples are present, and fill the empty values.
}

\section{Endpoint /{apiVersion}/operation/variant/family/index}{

Build the family index.
}

\section{Endpoint /{apiVersion}/operation/variant/index}{

Index variant files into the variant storage.
}

\section{Endpoint /{apiVersion}/operation/variant/index/launcher}{

Detect non-indexed VCF files in the study, and submit a job for indexing them.
}

\section{Endpoint /{apiVersion}/operation/variant/julie/run}{

Transform VariantStats into PopulationFrequency values and updates the VariantAnnotation.
}

\section{Endpoint /{apiVersion}/operation/variant/metadata/repair}{

Execute some repairs on Variant Storage Metadata. Advanced users only.
}

\section{Endpoint /{apiVersion}/operation/variant/metadata/synchronize}{

Synchronize catalog with variant storage metadata.
}

\section{Endpoint /{apiVersion}/operation/variant/sample/delete}{

Remove variant samples from the variant storage.
}

\section{Endpoint /{apiVersion}/operation/variant/sample/index}{

Build and annotate the sample index.
}

\section{Endpoint /{apiVersion}/operation/variant/sample/index/configure}{

Update SampleIndex configuration.
}

\section{Endpoint /{apiVersion}/operation/variant/score/delete}{

Remove a variant score in the database.
}

\section{Endpoint /{apiVersion}/operation/variant/score/index}{

Index a variant score in the database.
}

\section{Endpoint /{apiVersion}/operation/variant/secondaryIndex}{

Creates a secondary index using a search engine. If samples are provided, sample data will be added to the secondary index.
}

\section{Endpoint /{apiVersion}/operation/variant/secondaryIndex/delete}{

Remove a secondary index from the search engine for a specific set of samples.
}

\section{Endpoint /{apiVersion}/operation/variant/stats/delete}{

Deletes the VariantStats of a cohort/s from the database.
}

\section{Endpoint /{apiVersion}/operation/variant/stats/index}{

Compute variant stats for any cohort and any set of variants and index the result in the variant storage database.
}

\section{Endpoint /{apiVersion}/operation/variant/study/delete}{

Remove whole study from the variant storage.
}

\seealso{
\url{http://docs.opencb.org/display/opencga/Using+OpenCGA} and the RESTful API documentation
\url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}
\link{*}: Required parameter
}
