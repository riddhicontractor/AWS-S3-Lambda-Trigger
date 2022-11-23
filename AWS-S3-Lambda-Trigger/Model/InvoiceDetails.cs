using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
public class InvoiceDetails
{
    public int Id { get; set; }
    public string? InvoiceNumber { get; set; }
    public string? VendorName { get; set; }
    public string? ReceiverName { get; set; }
    public DateTime CreatedAt { get; set; }
    public string? FileName { get; set; }
}