using Orleans;
using Orleans.Runtime;

namespace Shiny.DocumentDb.Orleans;

internal sealed class DocumentDbGrainStorageOptionsValidator : IConfigurationValidator
{
    readonly DocumentDbGrainStorageOptions options;
    readonly string name;

    public DocumentDbGrainStorageOptionsValidator(DocumentDbGrainStorageOptions options, string name)
    {
        this.options = options;
        this.name = name;
    }

    public void ValidateConfiguration()
    {
        if (this.options.StoreFactory is null && this.options.DatabaseProvider is null)
            throw new OrleansConfigurationException(
                $"DocumentDb grain storage provider '{this.name}' requires either a DatabaseProvider " +
                $"(relational backend) or a StoreFactory (Cosmos/Mongo/etc.) to be configured.");
    }
}
